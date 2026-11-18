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

	"golang.org/x/sync/errgroup"

	"github.com/ilyashilyaev/notification-service/internal/events"
	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/postgres"
	"github.com/ilyashilyaev/notification-service/internal/server"
	"github.com/ilyashilyaev/notification-service/internal/storage"
	"github.com/ilyashilyaev/notification-service/internal/worker"
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

	// Kafka: консьюмер читает order-события (вход), продьюсер нужен relay для
	// публикации статус-событий из outbox (выход).
	kcfg := events.ConfigFromEnv()
	kclient, err := events.NewConsumerClient(ctx, kcfg)
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer kclient.Close()

	producer, err := events.NewProducerClient(ctx, kcfg)
	if err != nil {
		return fmt.Errorf("connect kafka producer: %w", err)
	}
	defer producer.Close()

	channelNames := make([]string, len(channels))
	for i, ch := range channels {
		channelNames[i] = ch.Name()
	}

	srv := server.New(addr, logger, repo, channels)
	wrk := worker.New(repo, channels, logger, worker.Config{StatusTopic: kcfg.StatusTopic})
	consumer := events.NewConsumer(kclient, repo, channelNames, logger)
	relay := events.NewRelay(events.NewKafkaPublisher(producer), repo, logger)

	// errgroup: все компоненты живут, пока жив gctx; падение или отмена одного
	// отменяет gctx -> остальные тоже корректно останавливаются. g.Wait ждёт всех.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(gctx) })
	g.Go(func() error { return wrk.Run(gctx) })
	g.Go(func() error { return consumer.Run(gctx) })
	g.Go(func() error { return relay.Run(gctx) })
	return g.Wait()
}
