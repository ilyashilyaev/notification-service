.DEFAULT_GOAL := help

# Адрес HTTP-сервера и строка подключения к БД.
# Значения по умолчанию совпадают с docker-compose.yml.
HTTP_ADDR      ?= :8080
DATABASE_URL   ?= postgres://notif:notif@localhost:5433/notifications?sslmode=disable
MIGRATIONS_DIR ?= migrations

.PHONY: help
help: ## Показать список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: db-up
db-up: ## Поднять PostgreSQL в docker и дождаться готовности
	docker compose up -d db
	@echo "ждём, пока база примет подключения..."
	@until docker compose exec -T db pg_isready -U notif -d notifications >/dev/null 2>&1; do sleep 0.5; done
	@echo "база готова"

.PHONY: db-down
db-down: ## Остановить PostgreSQL
	docker compose down

.PHONY: run
run: db-up ## Поднять базу и запустить сервер
	HTTP_ADDR=$(HTTP_ADDR) DATABASE_URL=$(DATABASE_URL) go run ./cmd/notification-service

.PHONY: migrate-up
migrate-up: ## Накатить миграции (нужен golang-migrate: brew install golang-migrate)
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Откатить последнюю миграцию
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" down 1

.PHONY: migrate-create
migrate-create: ## Новая миграция: make migrate-create name=add_x
	migrate create -ext sql -dir $(MIGRATIONS_DIR) -seq $(name)

.PHONY: build
build: ## Собрать бинарник в ./bin
	go build -o bin/notification-service ./cmd/notification-service

.PHONY: test
test: ## Прогнать тесты с гонками (-p 1: интеграционные тесты делят одну БД)
	go test -race -p 1 ./...

.PHONY: fmt
fmt: ## gofmt
	gofmt -w .

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: lint
lint: ## golangci-lint
	golangci-lint run ./...

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: check
check: fmt vet lint test ## Полная проверка перед коммитом
