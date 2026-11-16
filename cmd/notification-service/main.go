// Command notification-service — HTTP-сервер уведомлений.
//
// Вечер 1: пустой сервер с graceful shutdown. БД поднимается docker-compose,
// но код к ней ещё не подключается (это Вечер 2: миграции + pgx).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ilyashilyaev/notification-service/internal/server"
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

	// Ctrl+C / SIGTERM отменяют ctx -> сервер уходит в graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return server.New(addr, logger).Run(ctx)
}
