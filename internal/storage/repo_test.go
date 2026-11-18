package storage

import (
	"context"
	"os"
	"testing"

	"github.com/ilyashilyaev/notification-service/internal/postgres"
)

// testRepo подключается к реальной БД (DATABASE_URL или дефолт из docker-compose),
// чистит таблицы и возвращает репозиторий. Если базы нет — тест пропускается.
// Схема должна быть накатана: make db-up && make migrate-up.
func testRepo(t *testing.T) *Repo {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5433/notifications?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		t.Skipf("нет доступной БД (%v); запусти: make db-up && make migrate-up", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, "TRUNCATE notifications, events, outbox RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate (миграции накатаны? make migrate-up): %v", err)
	}
	return NewRepo(pool)
}

func TestCreateAndGetNotification(t *testing.T) {
	ctx := context.Background()
	r := testRepo(t)

	ev, err := r.CreateEvent(ctx, Event{Type: "order.created", Payload: []byte(`{"order_id":1}`)})
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	if ev.ID == 0 {
		t.Fatal("event id не присвоен")
	}
	if ev.CreatedAt.IsZero() {
		t.Error("created_at не заполнен")
	}

	n, err := r.CreateNotification(ctx, Notification{EventID: ev.ID, Channel: "email"})
	if err != nil {
		t.Fatalf("CreateNotification: %v", err)
	}
	if n.ID == 0 {
		t.Fatal("notification id не присвоен")
	}
	if n.Status != StatusPending {
		t.Errorf("status = %q, want %q", n.Status, StatusPending)
	}
	if n.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", n.Attempts)
	}
	if n.NextAttemptAt.IsZero() {
		t.Error("next_attempt_at не заполнен")
	}

	got, err := r.GetNotification(ctx, n.ID)
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if got.ID != n.ID || got.EventID != ev.ID || got.Channel != "email" {
		t.Errorf("got %+v, want id=%d event_id=%d channel=email", got, n.ID, ev.ID)
	}
	if got.Status != StatusPending {
		t.Errorf("got.Status = %q, want pending", got.Status)
	}
}
