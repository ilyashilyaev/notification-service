package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repo — доступ к таблицам events и notifications через пул pgx.
type Repo struct {
	pool *pgxpool.Pool
}

func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

// CreateEvent вставляет событие и возвращает его с присвоенными id и created_at.
// Используется в тестах для посева; боевой путь приёма — IngestEvent (с дедупом).
func (r *Repo) CreateEvent(ctx context.Context, e Event) (Event, error) {
	err := r.pool.QueryRow(ctx,
		`INSERT INTO events (external_id, type, payload) VALUES ($1, $2, $3)
		     RETURNING id, created_at`,
		e.ExternalID, e.Type, e.Payload,
	).Scan(&e.ID, &e.CreatedAt)
	return e, err
}

// IngestEvent атомарно сохраняет событие и создаёт по одному pending-уведомлению
// на каждый канал — в ОДНОЙ транзакции. Дедуп по external_id: если событие с таким
// id уже есть, ничего не вставляет и возвращает created=false (идемпотентность —
// повторная доставка из Kafka не плодит дублей). created=true -> в возврате лежат
// заполненное событие и его уведомления.
func (r *Repo) IngestEvent(ctx context.Context, e Event, channels []string) (Event, []Notification, bool, error) {
	var notes []Notification
	// Транзакция: событие и все его уведомления появляются атомарно (или ничего).
	// defer Rollback безопасен — после Commit он станет no-op (идиома pgx).
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Event{}, nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Вставка с дедупом: при конфликте по external_id строка не вставляется,
	// RETURNING ничего не отдаёт -> Scan возвращает pgx.ErrNoRows = это дубль.
	err = tx.QueryRow(ctx, "INSERT INTO events (external_id, type, payload) VALUES ($1,$2,$3)"+
		" ON CONFLICT (external_id) DO NOTHING  RETURNING id, created_at",
		e.ExternalID, e.Type, e.Payload).Scan(&e.ID, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, nil, false, nil // дубль — идемпотентно пропускаем
	}
	if err != nil {
		return Event{}, nil, false, err
	}

	// По одному pending-уведомлению на канал — в той же транзакции (event_id = FK e.ID).
	for _, channel := range channels {
		var n Notification
		err = tx.QueryRow(ctx, `INSERT INTO notifications (event_id, channel) VALUES ($1,$2)
        RETURNING id, status, attempts, next_attempt_at, created_at, updated_at`,
			e.ID, channel).Scan(&n.ID, &n.Status, &n.Attempts, &n.NextAttemptAt, &n.CreatedAt, &n.UpdatedAt)
		if err != nil {
			return Event{}, nil, false, err
		}
		n.EventID = e.ID
		n.Channel = channel
		notes = append(notes, n)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return Event{}, nil, false, err
	}
	return e, notes, true, nil
}

// CreateNotification вставляет уведомление. status/attempts/next_attempt_at
// берут значения по умолчанию из схемы (pending / 0 / now()) и возвращаются через RETURNING.
func (r *Repo) CreateNotification(ctx context.Context, n Notification) (Notification, error) {
	err := r.pool.QueryRow(ctx, `INSERT INTO notifications (event_id, channel)
									 	VALUES ($1, $2)
										RETURNING id, status, attempts, next_attempt_at, created_at, updated_at`,
		n.EventID, n.Channel).Scan(&n.ID, &n.Status, &n.Attempts, &n.NextAttemptAt, &n.CreatedAt, &n.UpdatedAt)
	return n, err
}

// GetNotification читает уведомление по id. Если строки нет — вернётся pgx.ErrNoRows;
// вызывающий отличит его через errors.Is и решит, что это 404. COALESCE подменяет
// nullable last_error пустой строкой, чтобы Scan в string не падал на NULL.
func (r *Repo) GetNotification(ctx context.Context, id int64) (Notification, error) {
	var n Notification
	err := r.pool.QueryRow(ctx, `SELECT id, event_id, channel, status, attempts, next_attempt_at,
       									COALESCE(last_error, '') , created_at, updated_at
										FROM notifications
										WHERE id = $1`, id).Scan(&n.ID, &n.EventID, &n.Channel, &n.Status, &n.Attempts,
		&n.NextAttemptAt, &n.LastError, &n.CreatedAt, &n.UpdatedAt)
	return n, err
}

// ClaimDue атомарно забирает до limit готовых к отправке pending-уведомлений,
// увеличивает attempts и отодвигает next_attempt_at на lease вперёд (visibility
// timeout). SKIP LOCKED во вложенном SELECT даёт нескольким воркерам брать разные
// строки без блокировки друг друга. Lease закрывает окно между захватом и финальным
// MarkSent/Mark*: пока строка в обработке, её next_attempt_at в будущем, так что
// другая горутина её не подхватит. Если воркер упадёт, не доведя строку, через lease
// она снова станет видимой — это и есть восстановление (at-least-once доставка).
func (r *Repo) ClaimDue(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := r.pool.Query(ctx, `
		UPDATE notifications
		SET attempts = attempts + 1,
		    next_attempt_at = now() + interval '1 minute',
		    updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING id, event_id, channel, status, attempts, next_attempt_at,
		          COALESCE(last_error, ''), created_at, updated_at`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Notification
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.EventID, &n.Channel, &n.Status, &n.Attempts,
			&n.NextAttemptAt, &n.LastError, &n.CreatedAt, &n.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// MarkSent помечает уведомление отправленным.
func (r *Repo) MarkSent(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications SET status = 'sent', updated_at = now() WHERE id = $1`, id)
	return err
}

// MarkRetry откладывает следующую попытку (status остаётся pending).
func (r *Repo) MarkRetry(ctx context.Context, id int64, next time.Time, lastErr string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications SET next_attempt_at = $2, last_error = $3, updated_at = now()
		 WHERE id = $1`, id, next, lastErr)
	return err
}

// MarkFailed помечает уведомление окончательно неуспешным.
func (r *Repo) MarkFailed(ctx context.Context, id int64, lastErr string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE notifications SET status = 'failed', last_error = $2, updated_at = now()
		 WHERE id = $1`, id, lastErr)
	return err
}
