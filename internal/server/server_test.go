package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/postgres"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// GET /healthz не трогает БД — хватает сервера без зависимостей.
func TestHealthz(t *testing.T) {
	srv := New(":0", discardLogger(), nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}
}

// realServer поднимает сервер с реальным репозиторием. Пропускается без БД.
// Схема должна быть накатана: make db-up && make migrate-up.
func realServer(t *testing.T) (*Server, *pgxpool.Pool) {
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

	log := discardLogger()
	channels := []notify.Channel{notify.NewEmailChannel(log), notify.NewPushChannel(log)}
	return New(":0", log, storage.NewRepo(pool), channels), pool
}

// POST /events создаёт событие и по уведомлению на каждый канал (email + push => 2).
// Повтор того же event_id идемпотентен: 200 duplicate, в базе ничего не прибавляется.
func TestCreateEvent(t *testing.T) {
	srv, pool := realServer(t)
	ctx := context.Background()

	body := `{"event_id":"evt-1","type":"order.created","payload":{"order_id":42}}`
	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
		rec := httptest.NewRecorder()
		srv.routes().ServeHTTP(rec, req)
		return rec
	}

	// Первый раз — создаётся (201).
	if rec := post(); rec.Code != http.StatusCreated {
		t.Fatalf("POST /events = %d, want 201 (body: %s)", rec.Code, rec.Body)
	}

	assertCounts := func(wantEvents, wantNotes int) {
		t.Helper()
		var events, notifications int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&events); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM notifications").Scan(&notifications); err != nil {
			t.Fatal(err)
		}
		if events != wantEvents {
			t.Errorf("events в базе = %d, want %d", events, wantEvents)
		}
		if notifications != wantNotes {
			t.Errorf("notifications в базе = %d, want %d (email+push)", notifications, wantNotes)
		}
	}
	assertCounts(1, 2)

	// Повтор того же event_id — дедуп: 200, счётчики не меняются.
	if rec := post(); rec.Code != http.StatusOK {
		t.Fatalf("повтор POST /events = %d, want 200 duplicate (body: %s)", rec.Code, rec.Body)
	}
	assertCounts(1, 2)
}
