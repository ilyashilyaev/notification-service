package events

import (
	"context"
	"encoding/json"
	"log/slog"

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
		// Штатная остановка по отмене ctx.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// PollFetches блокируется, пока не придут записи или не отменят ctx.
		fetches := c.client.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.log.Error("kafka fetches failed", "err", err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.handle(ctx, r); err != nil {
				c.log.Error("failed to handle record", "err", err)
			}
		})

		// Коммит оффсетов ПОСЛЕ handle — иначе падение между коммитом и записью
		// в БД потеряет сообщение.
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.Error("failed to commit uncommitted offsets", "err", err)
		}
	}
}

// handle декодирует одну запись и создаёт по ней event + уведомления.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) error {
	var oe OrderEvent
	err := json.Unmarshal(rec.Value, &oe)
	if err != nil {
		c.log.Error("failed to unmarshal record", "err", err)
		return nil
	}
	if oe.Type == "" {
		c.log.Error("empty event type, skipping")
		return nil
	}
	ev, err := c.repo.CreateEvent(ctx, storage.Event{Type: oe.Type, Payload: oe.Payload})
	if err != nil {
		c.log.Error("failed to create event", "err", err)
		return err
	}
	for _, ch := range c.channels {
		_, err := c.repo.CreateNotification(ctx, storage.Notification{
			EventID: ev.ID,
			Channel: ch,
		})
		if err != nil {
			c.log.Error("failed to create notification", "err", err)
			return err
		}
	}
	return nil
}
