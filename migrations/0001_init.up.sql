-- События заказа (order.created, order.paid, order.shipped, …).
CREATE TABLE events (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type       text        NOT NULL,
    payload    jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Уведомления, порождённые событием. Одно событие -> несколько уведомлений по каналам.
CREATE TABLE notifications (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        bigint      NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    channel         text        NOT NULL,
    status          text        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'sent', 'failed')),
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Воркер (Вечер 4) будет забирать готовые к отправке pending-уведомления
-- по next_attempt_at через SELECT … FOR UPDATE SKIP LOCKED. Частичный индекс
-- держит в себе только pending-строки — выборка дешёвая.
CREATE INDEX notifications_due_idx
    ON notifications (next_attempt_at)
    WHERE status = 'pending';
