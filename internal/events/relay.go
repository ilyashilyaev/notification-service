package events

import (
	"context"
	"log/slog"
	"time"

	"github.com/ilyashilyaev/notification-service/internal/metrics"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Relay — вторая половина transactional outbox. Воркер в одной транзакции со сменой
// статуса кладёт статус-событие в таблицу outbox; Relay вычитывает неопубликованные
// строки, публикует в Kafka и проставляет published_at. Порядок (сначала publish,
// потом MarkPublished) даёт at-least-once: при сбое строка опубликуется повторно,
// но не потеряется.
type Relay struct {
	pub       Publisher
	repo      *storage.Repo
	log       *slog.Logger
	batchSize int
	pollEvery time.Duration
}

func NewRelay(pub Publisher, repo *storage.Repo, log *slog.Logger) *Relay {
	return &Relay{pub: pub, repo: repo, log: log, batchSize: 100, pollEvery: time.Second}
}

// Run крутит слив outbox, пока не отменят ctx (по отмене возвращает nil).
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("outbox relay started")

	for {
		// Штатная остановка по отмене ctx.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		rows, err := r.repo.ListUnpublished(ctx, r.batchSize)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.log.Error("failed to list unpublished events", "err", err)
			if !r.sleep(ctx, r.pollEvery) {
				return nil
			}
			continue
		}

		if len(rows) == 0 {
			if !r.sleep(ctx, r.pollEvery) {
				return nil
			}
			continue
		}

		// Публикуем синхронно и копим успешно отданные. На ошибке прерываем пачку —
		// порядок важен, пропускать нельзя (следующая итерация начнёт с той же строки).
		var published []storage.OutboxRow
		for _, row := range rows {
			if err := r.pub.Publish(ctx, row.Topic, row.Key, row.Payload); err != nil {
				r.log.Error("failed to publish event", "err", err)
				break
			}
			published = append(published, row)
		}

		// Помечаем published ТОЛЬКО то, что реально ушло в Kafka (publish -> mark).
		for _, row := range published {
			if err := r.repo.MarkPublished(ctx, row.ID); err != nil {
				r.log.Error("failed to mark event published", "err", err)
				continue
			}
			// Считаем здесь (после успешного MarkPublished), а не сразу после Publish:
			// метрика должна отражать реально зафиксированную публикацию.
			metrics.OutboxPublished.Inc()
		}
	}
}

// sleep ждёт d или отмену ctx. true = поспали полностью, false = ctx отменён.
func (r *Relay) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
