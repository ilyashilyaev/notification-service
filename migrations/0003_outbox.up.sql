-- Transactional outbox: исходящие статус-события (notification.sent/failed).
-- Воркер пишет сюда строку в ТОЙ ЖЕ транзакции, что и смену статуса уведомления,
-- поэтому состояние и событие согласованы. Relay публикует строки в Kafka и
-- проставляет published_at.
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        text        NOT NULL,
    key          text        NOT NULL DEFAULT '',
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Relay забирает неопубликованные по порядку (FIFO); частичный индекс держит
-- только их, так что выборка дешёвая даже когда опубликованных миллионы.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
