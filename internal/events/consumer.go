package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/ilyashilyaev/notification-service/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Consumer читает order-события из Kafka и на каждое создаёт event + по одному
// pending-уведомлению на канал. Оффсет коммитится вручную ПОСЛЕ успешной записи
// в БД (клиент создан с DisableAutoCommit) — это даёт at-least-once.
type Consumer struct {
	client   *kgo.Client
	repo     *storage.Repo
	channels []string // имена каналов, как в server/worker
	log      *slog.Logger
}

func NewConsumer(client *kgo.Client, repo *storage.Repo, channels []string, log *slog.Logger) *Consumer {
	return &Consumer{client: client, repo: repo, channels: channels, log: log}
}

// Run крутит цикл чтения, пока не отменят ctx. По отмене возвращает nil
// (штатная остановка, как Server.Run / Worker.Run).
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("kafka consumer started", "channels", c.channels)

	for {
		// Штатная остановка по отмене ctx (как Worker.Run).
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// PollFetches блокируется, пока не придут записи или не отменят ctx.
		fetches := c.client.PollFetches(ctx)

		// Ошибки fetch разбираем до записей: отмена ctx -> штатный выход, иначе лог.
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return nil
			}
			for _, err := range errs {
				c.log.Error("kafka fetches failed", "err", err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.handle(ctx, r); err != nil {
				c.log.Error("failed to handle record", "err", err)
			}
		})

		// Коммит оффсетов ПОСЛЕ handle: падение между коммитом и записью в БД
		// потеряло бы сообщение. При повторной доставке спасает дедуп по event_id.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("failed to commit uncommitted offsets", "err", err)
		}
	}
}

// handle декодирует одну запись и через IngestEvent идемпотентно создаёт по ней
// event + уведомления. Битый JSON пропускаем (лог + nil, иначе "ядовитое"
// сообщение перечитается навсегда); сбой БД возвращаем (оффсет не коммитим,
// перечитаем позже — дедуп по event_id не даст дублей).
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	var oe OrderEvent
	if err := json.Unmarshal(rec.Value, &oe); err != nil {
		c.log.Error("failed to unmarshal record, skipping", "err", err)
		return nil
	}
	if oe.EventID == "" || oe.Type == "" {
		c.log.Error("record missing event_id or type, skipping",
			"event_id", oe.EventID, "type", oe.Type)
		return nil
	}
	_, _, created, err := c.repo.IngestEvent(ctx,
		storage.Event{ExternalID: oe.EventID, Type: oe.Type, Payload: oe.Payload},
		c.channels,
	)
	if err != nil {
		return fmt.Errorf("ingest event %s: %w", oe.EventID, err)
	}
	if !created {
		metrics.EventsIngested.WithLabelValues("kafka", "duplicate").Inc()
		c.log.Info("duplicate event skipped", "event_id", oe.EventID)
		return nil
	}
	metrics.EventsIngested.WithLabelValues("kafka", "created").Inc()
	return nil
}
