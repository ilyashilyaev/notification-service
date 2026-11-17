package events

import "encoding/json"

// OrderEvent — формат сообщения в топике order.events: что присылает сервис
// заказов. Payload пробрасывается в уведомления как есть (jsonb).
//
// Ключ дедупликации (event_id) добавим на Вечере 3 — вместе со схемой,
// unique-ограничением и идемпотентной вставкой в одной транзакции.
type OrderEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
