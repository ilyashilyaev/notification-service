package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"
)

// newTestLimiter подключается к Redis (REDIS_ADDR или localhost:6379). Без Redis
// тест пропускается: make redis-up поднимет контейнер.
func newTestLimiter(t *testing.T, limit int, window time.Duration) *Limiter {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb, err := NewClient(context.Background(), addr)
	if err != nil {
		t.Skipf("нет Redis (%v); make redis-up", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, limit, window)
}

func TestAllowWithinLimit(t *testing.T) {
	ctx := context.Background()
	l := newTestLimiter(t, 3, time.Second)
	key := "test:within:" + time.Now().Format("150405.000000")

	// Первые limit запросов проходят.
	for i := 1; i <= 3; i++ {
		allowed, _, err := l.Allow(ctx, key)
		if err != nil {
			t.Fatalf("Allow #%d: %v", i, err)
		}
		if !allowed {
			t.Fatalf("Allow #%d = false, want true (в пределах лимита)", i)
		}
	}

	// Следующий — за лимитом: отказ и положительный retryAfter.
	allowed, retryAfter, err := l.Allow(ctx, key)
	if err != nil {
		t.Fatalf("Allow #4: %v", err)
	}
	if allowed {
		t.Fatalf("Allow #4 = true, want false (лимит исчерпан)")
	}
	if retryAfter <= 0 {
		t.Fatalf("retryAfter = %v, want > 0", retryAfter)
	}
}

func TestAllowResetsAfterWindow(t *testing.T) {
	ctx := context.Background()
	l := newTestLimiter(t, 2, 400*time.Millisecond)
	key := "test:reset:" + time.Now().Format("150405.000000")

	for i := 1; i <= 2; i++ {
		if allowed, _, err := l.Allow(ctx, key); err != nil || !allowed {
			t.Fatalf("Allow #%d: allowed=%v err=%v, want true/nil", i, allowed, err)
		}
	}
	if allowed, _, _ := l.Allow(ctx, key); allowed {
		t.Fatalf("Allow #3 = true, want false (лимит исчерпан)")
	}

	// После окна ключ протух -> счётчик обнулился, снова пускает.
	time.Sleep(500 * time.Millisecond)
	if allowed, _, err := l.Allow(ctx, key); err != nil || !allowed {
		t.Fatalf("Allow после окна: allowed=%v err=%v, want true/nil", allowed, err)
	}
}
