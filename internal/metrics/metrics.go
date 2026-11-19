// Package metrics — Prometheus-метрики сервиса.
//
// Метрики объявляются через promauto: он сам регистрирует коллектор в дефолтном
// регистре (prometheus.DefaultRegisterer), поэтому отдельный MustRegister не нужен.
// Отдаются на GET /metrics через promhttp.Handler() (см. server.go).
//
// Аналогия с Ruby: это как инициализировать глобальные счётчики Prometheus::Client
// один раз при загрузке — здесь роль «загрузки» играют package-level var.
//
// Типы коллекторов (pkg.go.dev/github.com/prometheus/client_golang/prometheus):
//
//	CounterVec   — счётчик (только растёт), с лейблами; .WithLabelValues(...).Inc()
//	HistogramVec — распределение (длительности), с лейблами; .WithLabelValues(...).Observe(sec)
//	Counter      — счётчик без лейблов; .Inc()
//
// promauto: pkg.go.dev/github.com/prometheus/client_golang/prometheus/promauto
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "notif",
	Name:      "http_requests_total",
	Help:      "Number of HTTP requests by path, method and status.",
}, []string{"path", "method", "status"})

var HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "notif",
	Name:      "http_request_duration_seconds",
	Help:      "HTTP request duration in seconds.",
	Buckets:   prometheus.DefBuckets,
}, []string{"path", "method"})
var NotificationsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "notif",
	Name:      "notifications_processed_total",
	Help:      "Notification processed by channel and result (sent|failed|retry)",
}, []string{"channel", "result"})
var EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "notif",
	Name:      "events_ingested_total",
	Help:      "Order events ingested by source (http|kafka) and result (created|duplicate).",
}, []string{"source", "result"})

var OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "notif",
	Name:      "outbox_published_total",
	Help:      "Outbox rows published to Kafka.",
})
