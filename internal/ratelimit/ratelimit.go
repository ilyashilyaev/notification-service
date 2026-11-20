package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter — fixed-window counter: окно длиной window, не больше limit запросов
// на ключ за окно.
//
// Как работает одно окно: на каждый запрос инкрементируем счётчик ключа. На
// самом первом инкременте (счётчик стал 1) вешаем на ключ TTL = window. Через
// window ключ исчезает сам — счётчик обнуляется, окно «съезжает» скачком.
// Запрос проходит, пока счётчик <= limit.
//
// Почему INCR и PEXPIRE должны быть атомарны: если между ними сервис упадёт или
// другой инстанс вклинится, ключ может остаться без TTL и залипнуть навсегда.
// Поэтому оба шага выполняет один Lua-скрипт на стороне Redis (redis.NewScript +
// EVAL) — один round-trip, никаких гонок.
type Limiter struct {
	rdb    *redis.Client
	limit  int
	window time.Duration
}

// New собирает лимитер. limit — сколько запросов на ключ за window.
func New(rdb *redis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{rdb: rdb, limit: limit, window: window}
}

// allowScript атомарно инкрементирует счётчик ключа и на первом инкременте
// ставит TTL, затем возвращает {текущий счётчик, оставшийся TTL в мс}.
// KEYS[1] — ключ счётчика, ARGV[1] — длина окна в миллисекундах.
var allowScript = redis.NewScript(`
local n = redis.call("INCR", KEYS[1])
if n == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return {n, redis.call("PTTL", KEYS[1])}
`)

// Allow сообщает, пропустить ли очередной запрос от key, и (при отказе) сколько
// ждать до сброса окна.
func (l *Limiter) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error) {
	res, err := allowScript.Run(ctx, l.rdb, []string{key}, l.window.Milliseconds()).Result()
	if err != nil {
		return false, 0, err
	}
	vals := res.([]interface{})
	count := vals[0].(int64)
	ttlMs := vals[1].(int64)
	allowed = count <= int64(l.limit)
	if allowed {
		return allowed, 0, nil
	}
	return allowed, time.Duration(ttlMs) * time.Millisecond, nil
}
