package events

import "encoding/json"

// OrderEvent — формат сообщения в топике order.events: что присылает сервис
// заказов. Payload пробрасывается в уведомления как есть (jsonb).
//
// EventID — уникальный id события у отправителя; ключ дедупликации. Повторная
// доставка того же EventID (at-least-once в Kafka) не плодит дублей.
type OrderEvent struct {
	EventID string          `json:"event_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
