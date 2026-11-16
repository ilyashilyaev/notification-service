package storage

import (
	"context"

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
func (r *Repo) CreateEvent(ctx context.Context, e Event) (Event, error) {
	err := r.pool.QueryRow(ctx,
		`INSERT INTO events (type, payload) VALUES ($1, $2)
		     RETURNING id, created_at`,
		e.Type, e.Payload,
	).Scan(&e.ID, &e.CreatedAt)
	return e, err
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
