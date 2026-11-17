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
func TestCreateEvent(t *testing.T) {
	srv, pool := realServer(t)

	body := `{"type":"order.created","payload":{"order_id":42}}`
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /events = %d, want 201 (body: %s)", rec.Code, rec.Body)
	}

	var events, notifications int
	ctx := context.Background()
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM notifications").Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Errorf("events в базе = %d, want 1", events)
	}
	if notifications != 2 {
		t.Errorf("notifications в базе = %d, want 2 (email+push)", notifications)
	}
}
