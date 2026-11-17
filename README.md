# notification-service

Сервис уведомлений: принимает события заказа и асинхронно доставляет уведомления
по каналам (email, push, …) с повторами при сбоях. Pet-проект из плана развития
Go v2 — домен «Коммуникации по заказам» (Ozon, X5).

## Как устроено

```
POST /events ──► events ──┐
                          │ на каждый канал создаётся notification (status=pending)
                          ▼
                   notifications ◄──── worker (пул горутин)
                          │              забирает pending через FOR UPDATE SKIP LOCKED,
                          │              шлёт в канал, при ошибке — повтор с backoff,
                          ▼              после N попыток — failed
                  sent / failed
```

- **HTTP** (`internal/server`) — принимает события, создаёт уведомления.
- **PostgreSQL** (`internal/storage`, доступ через `pgx`) — хранит `events` и `notifications`.
- **Worker** (`internal/worker`) — пул горутин, тянет готовые уведомления и отправляет.
- **Channel** (`internal/notify`) — интерфейс канала доставки; сейчас email/push — заглушки.

Доставка — **at-least-once**: `ClaimDue` ставит lease (`next_attempt_at` в будущее),
так что при штатной работе уведомление уходит один раз, а при падении воркера
становится снова видимым и повторяется.

## Схема БД

| notifications | |
|---|---|
| `status` | `pending` → `sent` / `failed` |
| `attempts` | число попыток (растёт при захвате воркером) |
| `next_attempt_at` | когда можно забрать; при ретрае отодвигается на backoff |
| `last_error` | текст последней ошибки отправки |

Миграции — в `migrations/` (golang-migrate).

## Требования

- Go 1.25+
- Docker + docker compose (PostgreSQL); подойдёт colima
- [golang-migrate](https://github.com/golang-migrate/migrate) CLI — `brew install golang-migrate`
- golangci-lint — для `make lint`

## Запуск

```bash
make db-up
```

```bash
make migrate-up
```

```bash
make run
```

Сервер слушает `:8080`, воркер стартует вместе с ним. База — на `localhost:5433`
(5432 занят локальным Postgres; порт задаётся в `docker-compose.yml` и `DATABASE_URL`).

### Пример

```bash
curl -s -XPOST localhost:8080/events \
  -d '{"type":"order.paid","payload":{"order_id":42}}'
```

Создаст событие и по уведомлению на каждый канал; воркер доведёт их до `sent`.
Проверить живость: `curl localhost:8080/healthz`.

## Команды

```bash
make help     # список команд
make test     # go test -race -p 1 ./...  (интеграционные тесты делят одну БД)
make check    # fmt + vet + lint + test перед коммитом
make db-down  # остановить PostgreSQL
```

## Тесты

Требуют поднятой БД с накатанными миграциями (`make db-up && make migrate-up`);
без доступной базы интеграционные тесты пропускаются (`t.Skip`). Покрыто:
repository на реальной БД, `POST /events`, воркер (успех, провал после N попыток,
повтор-до-успеха).

## Структура

```
cmd/notification-service/   точка входа; запускает сервер + воркер через errgroup
internal/server/            HTTP: POST /events, /healthz, graceful shutdown
internal/storage/           модели + репозиторий на pgx (ClaimDue c SKIP LOCKED)
internal/worker/            пул горутин: отправка с ретраями и backoff
internal/notify/            интерфейс Channel + заглушки email/push
internal/postgres/          пул соединений pgx
migrations/                 схема БД (golang-migrate)
```

## Что дальше (план v2)

| Неделя | Блок |
|---|---|
| 2–3 | Kafka-консьюмер событий, transactional outbox, дедупликация по `event_id` |
| 4–5 | gRPC-статусы, Redis rate limit, метрики Prometheus, slog, pprof, нагрузка k6 |
| 6–8 | Деплой в Kubernetes (kind), GitHub Actions CI, финальный README со схемой |
