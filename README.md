# notification-service

Сервис уведомлений: принимает события заказа (по HTTP и из Kafka) и асинхронно
доставляет уведомления по каналам (email, push, …) с повторами при сбоях.
Pet-проект из плана развития Go v2 — домен «Коммуникации по заказам» (Ozon, X5).

## Как устроено

```
POST /events ───────┐
                    │  (оба входа создают event + по notification на канал)
Kafka order.events ─┤        дедуп по event_id (идемпотентно)
  (consumer group)  ▼
                 events ──► notifications (status=pending)
                                 │
                                 ▼
                 worker (пул горутин): забирает pending через
                 FOR UPDATE SKIP LOCKED, шлёт в канал, при ошибке —
                 повтор с backoff, после N попыток — failed.
                 Смену статуса + статус-событие пишет в ОДНОЙ транзакции:
                                 │
                   notifications (sent/failed)  +  outbox
                                                     │
                                                     ▼
                              relay: читает неопубликованные строки outbox,
                              публикует в Kafka (notification.status),
                              ставит published_at (at-least-once).
```

- **HTTP** (`internal/server`) — `POST /events` принимает событие.
- **Kafka-консьюмер** (`internal/events`) — читает те же события из топика `order.events`.
- **PostgreSQL** (`internal/storage`, доступ через `pgx`) — `events`, `notifications`, `outbox`.
- **Worker** (`internal/worker`) — пул горутин, отправляет уведомления.
- **Relay** (`internal/events`) — вторая половина transactional outbox: публикует статус-события в Kafka.
- **Channel** (`internal/notify`) — интерфейс канала доставки; email/push пока заглушки.

### Гарантии

- **Дедупликация.** `event_id` отправителя хранится как `events.external_id` (UNIQUE).
  Приём идёт через `IngestEvent` — одна транзакция со вставкой `ON CONFLICT DO NOTHING`:
  повторная доставка того же события (at-least-once в Kafka) не плодит дублей.
- **Доставка уведомлений — at-least-once.** `ClaimDue` ставит lease (`next_attempt_at`
  в будущее): при штатной работе уведомление уходит один раз, при падении воркера
  снова становится видимым и повторяется.
- **Transactional outbox.** Смена статуса (`sent`/`failed`) и запись статус-события
  в `outbox` — в одной транзакции, поэтому «статус поменяли, а событие не отдали»
  (или наоборот) невозможно. Relay публикует строки в Kafka отдельно: publish →
  потом `published_at` (не ушло в Kafka — не пометили published).

## Схема БД

| таблица | ключевое |
|---|---|
| `events.external_id` | event_id отправителя, UNIQUE — ключ дедупа |
| `notifications.status` | `pending` → `sent` / `failed` |
| `notifications.attempts` | число попыток (растёт при захвате воркером) |
| `notifications.next_attempt_at` | когда можно забрать; при ретрае отодвигается на backoff |
| `notifications.last_error` | текст последней ошибки отправки |
| `outbox` | исходящие статус-события: `topic`, `key`, `payload`, `published_at` |

Миграции — в `migrations/` (golang-migrate).

## Требования

- Go 1.26+
- Docker + docker compose (PostgreSQL + Kafka); подойдёт colima
- [golang-migrate](https://github.com/golang-migrate/migrate) CLI — `brew install golang-migrate`
- golangci-lint — для `make lint`

## Запуск

```bash
make infra-up
```

```bash
make migrate-up
```

```bash
make run
```

`make infra-up` поднимает PostgreSQL и Kafka и создаёт топики (`order.events`,
`notification.status`). Сервер слушает `:8080`, воркер, консьюмер и relay стартуют
вместе с ним. База — на `localhost:5433` (5432 занят локальным Postgres), Kafka — на
`localhost:9092`. Адреса задаются в `docker-compose.yml` и переменных окружения
(`DATABASE_URL`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_STATUS_TOPIC`).

### Пример

Вход по HTTP (`event_id` обязателен — ключ дедупа):

```bash
curl -s -XPOST localhost:8080/events \
  -d '{"event_id":"order-42-paid","type":"order.paid","payload":{"order_id":42}}'
```

Вход из Kafka:

```bash
echo '{"event_id":"order-42-paid","type":"order.paid","payload":{"order_id":42}}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic order.events
```

Оба создадут событие и по уведомлению на каждый канал; воркер доведёт их до `sent`,
relay опубликует статус-события. Посмотреть статусы:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic notification.status --from-beginning
```

Проверить живость: `curl localhost:8080/healthz`.

## Команды

```bash
make help       # список команд
make infra-up   # PostgreSQL + Kafka + топики
make test       # go test -race -p 1 ./...  (интеграционные тесты делят одну БД)
make check      # fmt + vet + lint + test перед коммитом
make db-down    # остановить всё (docker compose down)
```

## Тесты

Требуют поднятой БД с накатанными миграциями (`make infra-up && make migrate-up`);
без доступной базы интеграционные тесты пропускаются (`t.Skip`). Kafka в тестах не
нужна — relay тестируется через фейковый `Publisher`. Покрыто: repository и outbox на
реальной БД, дедуп `POST /events`, воркер (успех, провал после N попыток,
повтор-до-успеха), relay (слив outbox).

## Структура

```
cmd/notification-service/   точка входа; сервер + worker + consumer + relay через errgroup
internal/server/            HTTP: POST /events, /healthz, graceful shutdown
internal/events/            Kafka: consumer (вход), relay + Publisher (выход), конфиг
internal/storage/           модели + репозиторий на pgx (IngestEvent, ClaimDue, outbox)
internal/worker/            пул горутин: отправка с ретраями и backoff
internal/notify/            интерфейс Channel + заглушки email/push
internal/postgres/          пул соединений pgx
migrations/                 схема БД (golang-migrate)
```

## Что дальше (план v2)

| Неделя | Блок | Статус |
|---|---|---|
| 2–3 | Kafka-консьюмер, дедуп по `event_id`, transactional outbox | ✅ готово |
| 4–5 | gRPC-статусы, Redis rate limit, метрики Prometheus, slog, pprof, нагрузка k6 | |
| 6–8 | Деплой в Kubernetes (kind), GitHub Actions CI, финальный README со схемой | |
