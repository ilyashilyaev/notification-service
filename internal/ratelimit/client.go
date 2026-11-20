// Package ratelimit — пер-юзер ограничение частоты запросов поверх Redis.
//
// Зачем Redis, а не счётчик в памяти: инстансов сервиса может быть несколько
// (горизонтальное масштабирование), а лимит должен быть общим на пользователя.
// Общий счётчик живёт во внешнем хранилище — Redis, атомарные операции которого
// (INCR, PEXPIRE) делают инкремент безопасным без блокировок на стороне Go.
//
// go-redis: pkg.go.dev/github.com/redis/go-redis/v9
package ratelimit

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// NewClient подключается к Redis по адресу addr ("host:port") и проверяет связь
// пингом (как postgres.NewPool делает для БД). Готовый клиент потокобезопасен —
// один на весь процесс.
func NewClient(ctx context.Context, addr string) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return rdb, nil
}
