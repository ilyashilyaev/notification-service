.DEFAULT_GOAL := help

# Адрес HTTP-сервера и строка подключения к БД.
# Значения по умолчанию совпадают с docker-compose.yml.
HTTP_ADDR      ?= :8080
GRPC_ADDR      ?= :9090
DATABASE_URL   ?= postgres://notif:notif@localhost:5433/notifications?sslmode=disable
MIGRATIONS_DIR ?= migrations
KAFKA_BROKERS      ?= localhost:9092
KAFKA_TOPIC        ?= order.events
KAFKA_STATUS_TOPIC ?= notification.status
REDIS_ADDR     ?= localhost:6379
PROTO_DIR      ?= proto

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

.PHONY: kafka-up
kafka-up: ## Поднять Kafka в docker и дождаться готовности
	docker compose up -d kafka
	@echo "ждём, пока Kafka примет подключения..."
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 1; done
	@echo "Kafka готова"

.PHONY: kafka-topic
kafka-topic: ## Создать топики order-событий и статусов (идемпотентно)
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic $(KAFKA_TOPIC) --partitions 3 --replication-factor 1
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic $(KAFKA_STATUS_TOPIC) --partitions 3 --replication-factor 1

.PHONY: redis-up
redis-up: ## Поднять Redis в docker и дождаться готовности
	docker compose up -d redis
	@echo "ждём, пока Redis примет подключения..."
	@until docker compose exec -T redis redis-cli ping >/dev/null 2>&1; do sleep 0.5; done
	@echo "Redis готов"

.PHONY: infra-up
infra-up: db-up kafka-up kafka-topic redis-up ## Поднять базу, Kafka и Redis, создать топик

.PHONY: run
run: infra-up ## Поднять инфру и запустить сервер
	HTTP_ADDR=$(HTTP_ADDR) GRPC_ADDR=$(GRPC_ADDR) DATABASE_URL=$(DATABASE_URL) \
	KAFKA_BROKERS=$(KAFKA_BROKERS) KAFKA_TOPIC=$(KAFKA_TOPIC) \
	KAFKA_STATUS_TOPIC=$(KAFKA_STATUS_TOPIC) REDIS_ADDR=$(REDIS_ADDR) \
	go run ./cmd/notification-service

.PHONY: migrate-up
migrate-up: ## Накатить миграции (нужен golang-migrate: brew install golang-migrate)
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Откатить последнюю миграцию
	migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" down 1

.PHONY: migrate-create
migrate-create: ## Новая миграция: make migrate-create name=add_x
	migrate create -ext sql -dir $(MIGRATIONS_DIR) -seq $(name)

.PHONY: proto
proto: ## Сгенерировать Go-код из .proto (нужны protoc + protoc-gen-go[-grpc] в GOPATH/bin)
	PATH="$(shell go env GOPATH)/bin:$$PATH" protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=module=github.com/ilyashilyaev/notification-service \
		--go-grpc_out=. --go-grpc_opt=module=github.com/ilyashilyaev/notification-service \
		$(PROTO_DIR)/notification/v1/notification.proto

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
