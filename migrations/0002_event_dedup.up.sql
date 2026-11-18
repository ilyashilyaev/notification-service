-- external_id = event_id из входящего сообщения (Kafka/HTTP): уникальный id
-- события у отправителя. Ключ дедупликации: повторная доставка того же события
-- (at-least-once в Kafka) не должна плодить дублей уведомлений.
ALTER TABLE events ADD COLUMN external_id text;

-- Старым строкам (до дедупа) проставляем суррогатный уникальный ключ, иначе
-- NOT NULL + UNIQUE упадут на NULL.
UPDATE events SET external_id = 'legacy-' || id WHERE external_id IS NULL;

ALTER TABLE events ALTER COLUMN external_id SET NOT NULL;
ALTER TABLE events ADD CONSTRAINT events_external_id_key UNIQUE (external_id);
