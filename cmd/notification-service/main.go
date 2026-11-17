// Command notification-service — HTTP-сервер уведомлений.
//
// Вечер 1: пустой сервер с graceful shutdown. БД поднимается docker-compose,
// но код к ней ещё не подключается (это Вечер 2: миграции + pgx).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/postgres"
	"github.com/ilyashilyaev/notification-service/internal/server"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// run() держит все defer; os.Exit зовём только здесь, после их отработки.
	if err := run(logger); err != nil {
		logger.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://notif:notif@localhost:5433/notifications?sslmode=disable"
	}

	// Ctrl+C / SIGTERM отменяют ctx -> сервер уходит в graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	repo := storage.NewRepo(pool)
	channels := []notify.Channel{
		notify.NewEmailChannel(logger),
		notify.NewPushChannel(logger),
	}

	return server.New(addr, logger, repo, channels).Run(ctx)
}
