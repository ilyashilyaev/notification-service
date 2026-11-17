package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/postgres"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// okChannel всегда успешно; failChannel всегда падает — детерминированно.
type okChannel struct{ name string }

func (c okChannel) Name() string                                     { return c.name }
func (c okChannel) Send(context.Context, storage.Notification) error { return nil }

type failChannel struct{ name string }

func (c failChannel) Name() string                                     { return c.name }
func (c failChannel) Send(context.Context, storage.Notification) error { return errors.New("boom") }

func testDB(t *testing.T) (*storage.Repo, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5433/notifications?sslmode=disable"
	}
	pool, err := postgres.NewPool(context.Background(), dsn)
	if err != nil {
		t.Skipf("нет БД (%v); make db-up && make migrate-up", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), "TRUNCATE notifications, events RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return storage.NewRepo(pool), pool
}

// Воркер должен довести ВСЕ уведомления до терминального статуса: email -> sent,
// push (падает всегда) -> failed после MaxAttempts. Pending не остаётся.
func TestWorkerDrainsToTerminal(t *testing.T) {
	repo, pool := testDB(t)
	bg := context.Background()

	ev, err := repo.CreateEvent(bg, storage.Event{Type: "order.created", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range []string{"email", "push"} {
		if _, err := repo.CreateNotification(bg, storage.Notification{EventID: ev.ID, Channel: ch}); err != nil {
			t.Fatal(err)
		}
	}

	w := New(repo,
		[]notify.Channel{okChannel{"email"}, failChannel{"push"}},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config{PoolSize: 2, BatchSize: 5, PollEvery: 10 * time.Millisecond, MaxAttempts: 3, BaseBackoff: 5 * time.Millisecond},
	)

	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Ждём, пока не останется pending (или таймаут).
	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("остались pending-уведомления — воркер не доработал")
		}
		var pending int
		if err := pool.QueryRow(bg, "SELECT count(*) FROM notifications WHERE status = 'pending'").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run вернул ошибку: %v", err)
	}

	var sent, failed int
	_ = pool.QueryRow(bg, "SELECT count(*) FROM notifications WHERE status='sent'").Scan(&sent)
	_ = pool.QueryRow(bg, "SELECT count(*) FROM notifications WHERE status='failed'").Scan(&failed)
	if sent != 1 {
		t.Errorf("sent = %d, want 1 (email)", sent)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1 (push после %d попыток)", failed, 3)
	}
}
