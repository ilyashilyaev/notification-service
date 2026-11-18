// Package events — приём order-событий из Kafka (и, с Вечера 4, публикация статусов).
package events

import (
	"os"
	"strings"
)

// Config — параметры подключения к Kafka. GroupID задаёт consumer group: брокер
// хранит оффсеты по группе, так что рестарт сервиса продолжает читать с места,
// а несколько инстансов в одной группе делят партиции между собой.
type Config struct {
	Brokers     []string // адреса брокеров, напр. ["localhost:9092"]
	Topic       string   // топик order-событий (вход)
	GroupID     string   // consumer group
	StatusTopic string   // топик статус-событий (выход, outbox relay)
}

// ConfigFromEnv читает KAFKA_BROKERS (через запятую), KAFKA_TOPIC, KAFKA_GROUP_ID,
// KAFKA_STATUS_TOPIC. Пустые значения заменяются дефолтами из docker-compose/Makefile.
func ConfigFromEnv() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "order.events"
	}
	group := os.Getenv("KAFKA_GROUP_ID")
	if group == "" {
		group = "notification-service"
	}
	statusTopic := os.Getenv("KAFKA_STATUS_TOPIC")
	if statusTopic == "" {
		statusTopic = "notification.status"
	}
	return Config{
		Brokers:     strings.Split(brokers, ","),
		Topic:       topic,
		GroupID:     group,
		StatusTopic: statusTopic,
	}
}
