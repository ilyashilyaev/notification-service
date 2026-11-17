// Package notify — каналы доставки уведомлений.
package notify

import (
	"context"
	"log/slog"

	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Channel — способ доставки уведомления (email, push, …).
// Реальная отправка появится в Вечер 4 (воркер); пока это заглушки, нужные,
// чтобы знать набор каналов и создавать по уведомлению на каждый.
type Channel interface {
	Name() string
	Send(ctx context.Context, n storage.Notification) error
}

// EmailChannel — заглушка канала email.
type EmailChannel struct{ log *slog.Logger }

func NewEmailChannel(log *slog.Logger) *EmailChannel { return &EmailChannel{log: log} }

func (c *EmailChannel) Name() string { return "email" }

func (c *EmailChannel) Send(ctx context.Context, n storage.Notification) error {
	c.log.InfoContext(ctx, "email send (stub)", "notification_id", n.ID)
	return nil
}

// PushChannel — заглушка канала push.
type PushChannel struct{ log *slog.Logger }

func NewPushChannel(log *slog.Logger) *PushChannel { return &PushChannel{log: log} }

func (c *PushChannel) Name() string { return "push" }

func (c *PushChannel) Send(ctx context.Context, n storage.Notification) error {
	c.log.InfoContext(ctx, "push send (stub)", "notification_id", n.ID)
	return nil
}

// Проверка на этапе компиляции, что заглушки реализуют Channel.
var (
	_ Channel = (*EmailChannel)(nil)
	_ Channel = (*PushChannel)(nil)
)
