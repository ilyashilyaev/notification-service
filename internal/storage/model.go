// Package storage — доменные типы и доступ к БД (репозиторий на pgx).
package storage

import "time"

// Status — состояние уведомления. Совпадает с CHECK-ограничением в схеме.
type Status string

const (
	StatusPending Status = "pending"
	StatusSent    Status = "sent"
	StatusFailed  Status = "failed"
)

// Event — событие заказа.
type Event struct {
	ID         int64
	ExternalID string // event_id отправителя; ключ дедупа (UNIQUE в схеме)
	Type       string
	Payload    []byte // jsonb
	CreatedAt  time.Time
}

// Notification — уведомление, порождённое событием.
type Notification struct {
	ID            int64
	EventID       int64
	Channel       string
	Status        Status
	Attempts      int
	NextAttemptAt time.Time
	LastError     string // пусто, пока ошибок не было
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// OutboxRow — неопубликованная запись transactional outbox: что и куда отдать в Kafka.
type OutboxRow struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte // jsonb
}
