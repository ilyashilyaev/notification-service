package storage

import (
	"context"
	"encoding/json"
	"testing"
)

// MarkSent должен в одной транзакции перевести уведомление в sent И записать
// статус-событие в outbox (transactional outbox).
func TestMarkSentWritesOutbox(t *testing.T) {
	ctx := context.Background()
	r := testRepo(t)

	ev, err := r.CreateEvent(ctx, Event{ExternalID: "evt-ob-1", Type: "order.paid", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	n, err := r.CreateNotification(ctx, Notification{EventID: ev.ID, Channel: "email"})
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}

	if err := r.MarkSent(ctx, n, "notification.status"); err != nil {
		t.Fatalf("MarkSent: %v", err)
	}

	got, err := r.GetNotification(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if got.Status != StatusSent {
		t.Errorf("status = %q, want sent", got.Status)
	}

	rows, err := r.ListUnpublished(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnpublished: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("outbox строк = %d, want 1", len(rows))
	}
	if rows[0].Topic != "notification.status" {
		t.Errorf("topic = %q, want notification.status", rows[0].Topic)
	}

	var se struct {
		Status         string `json:"status"`
		Channel        string `json:"channel"`
		NotificationID int64  `json:"notification_id"`
		EventID        int64  `json:"event_id"`
	}
	if err := json.Unmarshal(rows[0].Payload, &se); err != nil {
		t.Fatalf("payload не JSON: %v", err)
	}
	if se.Status != "sent" || se.Channel != "email" || se.NotificationID != n.ID || se.EventID != ev.ID {
		t.Errorf("payload = %+v", se)
	}
}

// ListUnpublished отдаёт неопубликованные; после MarkPublished их больше нет.
func TestListAndMarkPublished(t *testing.T) {
	ctx := context.Background()
	r := testRepo(t)

	ev, err := r.CreateEvent(ctx, Event{ExternalID: "evt-ob-2", Type: "order.paid", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	n, err := r.CreateNotification(ctx, Notification{EventID: ev.ID, Channel: "push"})
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if err := r.MarkFailed(ctx, n, "boom", "notification.status"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	rows, err := r.ListUnpublished(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnpublished: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("неопубликованных = %d, want 1", len(rows))
	}

	if err := r.MarkPublished(ctx, rows[0].ID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	rows, err = r.ListUnpublished(ctx, 10)
	if err != nil {
		t.Fatalf("ListUnpublished (2): %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("после MarkPublished неопубликованных = %d, want 0", len(rows))
	}
}
