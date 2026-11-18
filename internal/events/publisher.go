package events

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher публикует одно сообщение в Kafka. Интерфейс нужен, чтобы Relay не
// зависел от *kgo.Client напрямую — в тестах подставляем фейк.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// kafkaPublisher — боевая реализация поверх franz-go: синхронная публикация.
type kafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher оборачивает клиента franz-go в Publisher.
func NewKafkaPublisher(client *kgo.Client) Publisher {
	return &kafkaPublisher{client: client}
}

func (p *kafkaPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}
