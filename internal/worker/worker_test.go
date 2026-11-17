package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
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

// flakyChannel падает первые failFirst раз, затем успешно. atomic — Send зовут из пула.
type flakyChannel struct {
	name      string
	failFirst int32
	calls     int32
}

func (c *flakyChannel) Name() string { return c.name }
func (c *flakyChannel) Send(context.Context, storage.Notification) error {
	if atomic.AddInt32(&c.calls, 1) <= c.failFirst {
		return errors.New("temporary")
	}
	return nil
}

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

// Канал, падающий первые 2 раза, на 3-й должен довести уведомление до sent.
func TestWorkerRetriesThenSucceeds(t *testing.T) {
	repo, pool := testDB(t)
	bg := context.Background()

	ev, err := repo.CreateEvent(bg, storage.Event{Type: "order.created", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateNotification(bg, storage.Notification{EventID: ev.ID, Channel: "email"}); err != nil {
		t.Fatal(err)
	}

	w := New(repo,
		[]notify.Channel{&flakyChannel{name: "email", failFirst: 2}},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config{PoolSize: 1, BatchSize: 5, PollEvery: 10 * time.Millisecond, MaxAttempts: 5, BaseBackoff: 5 * time.Millisecond},
	)

	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	var status string
	var attempts int
	for {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("не дошло до sent: status=%q attempts=%d", status, attempts)
		}
		_ = pool.QueryRow(bg, "SELECT status, attempts FROM notifications WHERE event_id=$1", ev.ID).
			Scan(&status, &attempts)
		if status == "sent" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if attempts < 3 {
		t.Errorf("attempts = %d, want >= 3 (2 провала + успех)", attempts)
	}
}
