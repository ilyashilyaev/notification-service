# notification-service

Сервис уведомлений: принимает события заказа и доставляет уведомления по каналам
(email, push, …). Pet-проект из плана развития Go v2 — домен «Коммуникации по заказам».

## Статус

**Неделя 1, Вечер 1 — MVP-каркас.** Сейчас это пустой HTTP-сервер + поднятая база.
Дальше по плану: миграции и `pgx` → `POST /events` → воркер с `SKIP LOCKED` и ретраями → Kafka.

## Требования

- Go 1.24+
- Docker + docker compose (для PostgreSQL)
- golangci-lint (для `make lint`)

## Запуск

```bash
make run
```

Поднимает PostgreSQL в docker и запускает сервер на `:8080`. Проверка живости:

```bash
curl localhost:8080/healthz
```

Остановить базу:

```bash
make db-down
```

## Команды

```bash
make help     # список команд
make check    # fmt + vet + lint + test перед коммитом
make test     # go test -race ./...
```

## Структура

```
cmd/notification-service/   точка входа (main)
internal/server/            HTTP-сервер и маршруты
docker-compose.yml          PostgreSQL для локальной разработки
```

## Планы (из плана v2)

| Неделя | Блок |
|---|---|
| 1 | MVP: Postgres, `POST /events`, воркер с ретраями, тесты, README |
| 2–3 | Kafka-консьюмер, transactional outbox, дедупликация по `event_id` |
| 4–5 | gRPC-статусы, Redis rate limit, метрики Prometheus, slog, pprof, k6 |
| 6–8 | Деплой в Kubernetes (kind), GitHub Actions CI |
