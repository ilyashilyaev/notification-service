package events

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewConsumerClient создаёт клиента franz-go, подписанного на cfg.Topic в группе
// cfg.GroupID, и проверяет связь с брокером (Ping).
//
// DisableAutoCommit — ключевое решение: оффсет коммитим вручную ПОСЛЕ успешной
// записи события в БД. При авто-коммите брокер мог бы продвинуть оффсет до того,
// как мы сохранили событие; падение в этот момент потеряло бы сообщение. Ручной
// коммит даёт at-least-once: при сбое событие прочитается заново, а дедуп по
// event_id (Вечер 3) гарантирует, что повтор не создаст дублей уведомлений.
func NewConsumerClient(ctx context.Context, cfg Config) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return cl, nil
}
