// Package worker — фоновая отправка pending-уведомлений.
package worker

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Config — настройки воркера. Нули заменяются дефолтами в New.
type Config struct {
	PoolSize    int           // число горутин-воркеров
	BatchSize   int           // сколько уведомлений забирать за один ClaimDue
	PollEvery   time.Duration // пауза, когда забирать нечего
	MaxAttempts int           // после скольких неудач -> failed
	BaseBackoff time.Duration // база экспоненциальной задержки
	StatusTopic string        // топик статус-событий (пишутся в outbox при sent/failed)
}

// Worker раскладывает pending-уведомления по каналам пулом горутин.
type Worker struct {
	repo     *storage.Repo
	channels map[string]notify.Channel
	log      *slog.Logger
	cfg      Config
}

func New(repo *storage.Repo, channels []notify.Channel, log *slog.Logger, cfg Config) *Worker {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = time.Second
	}
	if cfg.StatusTopic == "" {
		cfg.StatusTopic = "notification.status"
	}

	m := make(map[string]notify.Channel, len(channels))
	for _, c := range channels {
		m[c.Name()] = c
	}
	return &Worker{repo: repo, channels: m, log: log, cfg: cfg}
}

// backoff — экспоненциальная задержка перед следующей попыткой (attempts >= 1):
// base * 2^(attempts-1), но не больше 5 минут.
func (w *Worker) backoff(attempts int) time.Duration {
	d := float64(w.cfg.BaseBackoff) * math.Pow(2, float64(attempts-1))
	if max := float64(5 * time.Minute); d > max {
		d = max
	}
	return time.Duration(d)
}

// Run запускает пул из cfg.PoolSize горутин и работает, пока не отменят ctx.
// Каждая горутина забирает батч через ClaimDue и обрабатывает его; на пустой
// выборке делает прерываемую паузу. wg.Wait перед возвратом гарантирует, что
// Run завершится только когда все горутины реально встали (настоящий graceful stop).
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.PoolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// 1. отмена? -> выйти
				select {
				case <-ctx.Done():
					return
				default:
				}
				// 2. забрать батч
				batch, err := w.repo.ClaimDue(ctx, w.cfg.BatchSize)
				if err != nil {
					// при отмене ctx ClaimDue тоже вернёт err — не шуми, выйди
					if ctx.Err() != nil {
						return
					}
					w.log.Error("claim due", "err", err)
					// подождать и продолжить (см. хелпер ниже)
					if !sleepCtx(ctx, w.cfg.PollEvery) {
						return
					}
					continue
				}
				// 3. пусто -> пауза (прерываемая) и по новой
				if len(batch) == 0 {
					if !sleepCtx(ctx, w.cfg.PollEvery) {
						return
					}
					continue
				}
				// 4. обработать каждое
				for _, n := range batch {
					w.process(ctx, n)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

// process отправляет одно уведомление и фиксирует итог: успех -> sent,
// ошибка -> retry с backoff либо failed после MaxAttempts.
func (w *Worker) process(ctx context.Context, n storage.Notification) {
	ch, ok := w.channels[n.Channel]
	if !ok {
		if err := w.repo.MarkFailed(ctx, n, "unknown channel: "+n.Channel, w.cfg.StatusTopic); err != nil {
			w.log.Error("unknown channel", "channel", n.Channel)
		}
		return
	}
	err := ch.Send(ctx, n)
	if err == nil {
		if err := w.repo.MarkSent(ctx, n, w.cfg.StatusTopic); err != nil {
			w.log.Error("mark sent", "err", err)
		}
		return
	}
	if n.Attempts >= w.cfg.MaxAttempts {
		if e := w.repo.MarkFailed(ctx, n, err.Error(), w.cfg.StatusTopic); e != nil {
			w.log.Error("failed to mark failed attempt", "channel", n.Channel, "err", e)
		}
	} else {
		next := time.Now().Add(w.backoff(n.Attempts))
		if e := w.repo.MarkRetry(ctx, n.ID, next, err.Error()); e != nil {
			w.log.Error("failed to mark retry", "channel", n.Channel, "err", e)
		}
	}
}

// sleepCtx ждёт d или отмену ctx. true = поспали полностью, false = ctx отменён.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
