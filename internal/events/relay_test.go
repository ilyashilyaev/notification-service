package events

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ilyashilyaev/notification-service/internal/postgres"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakePublisher заменяет Kafka в тестах. failFrom>0 — начиная с какого вызова
// Publish падает (1 = падать сразу). published хранит ключи успешно отданных.
type fakePublisher struct {
	mu        sync.Mutex
	published []string
	failFrom  int
	calls     int
}

func (p *fakePublisher) Publish(_ context.Context, _, key string, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.failFrom > 0 && p.calls >= p.failFrom {
		return errors.New("publish failed")
	}
	p.published = append(p.published, key)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

// testRelayDB подключается к реальной БД (нужна для outbox), чистит таблицы.
func testRelayDB(t *testing.T) *storage.Repo {
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
	if _, err := pool.Exec(context.Background(), "TRUNCATE notifications, events, outbox RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return storage.NewRepo(pool)
}

// seedOutbox кладёт n статус-событий в outbox через MarkSent (боевой путь).
func seedOutbox(t *testing.T, repo *storage.Repo, n int) {
	t.Helper()
	ctx := context.Background()
	ev, err := repo.CreateEvent(ctx, storage.Event{ExternalID: "evt-relay", Type: "order.paid", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		note, err := repo.CreateNotification(ctx, storage.Notification{EventID: ev.ID, Channel: "email"})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkSent(ctx, note, "notification.status"); err != nil {
			t.Fatal(err)
		}
	}
}

// Relay должен опубликовать все неопубликованные строки и пометить их published.
func TestRelayDrainsOutbox(t *testing.T) {
	repo := testRelayDB(t)
	seedOutbox(t, repo, 3)

	pub := &fakePublisher{}
	relay := &Relay{pub: pub, repo: repo, log: discardLogger(), batchSize: 10, pollEvery: 5 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("outbox не слит за отведённое время")
		}
		rows, err := repo.ListUnpublished(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run вернул ошибку: %v", err)
	}

	if pub.count() != 3 {
		t.Errorf("опубликовано = %d, want 3", pub.count())
	}
}

// Если публикация падает — строки остаются неопубликованными (publish -> mark):
// не ушло в Kafka, значит published_at не ставим.
func TestRelayKeepsUnpublishedOnError(t *testing.T) {
	repo := testRelayDB(t)
	seedOutbox(t, repo, 2)

	pub := &fakePublisher{failFrom: 1} // падает с первого вызова
	relay := &Relay{pub: pub, repo: repo, log: discardLogger(), batchSize: 10, pollEvery: 20 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	time.Sleep(100 * time.Millisecond) // дать relay попытаться и упасть
	cancel()
	<-done

	if pub.count() != 0 {
		t.Errorf("опубликовано = %d, want 0 (publish падает)", pub.count())
	}
	rows, err := repo.ListUnpublished(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Errorf("неопубликовано = %d, want 2 (ничего не помечено published)", len(rows))
	}
}
