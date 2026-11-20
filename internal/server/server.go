// Package server — HTTP-сервер сервиса уведомлений.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/ilyashilyaev/notification-service/internal/metrics"
	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/ratelimit"
	"github.com/ilyashilyaev/notification-service/internal/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server оборачивает http.Server с маршрутами сервиса.
type Server struct {
	addr     string
	log      *slog.Logger
	repo     *storage.Repo
	channels []notify.Channel
	limiter  *ratelimit.Limiter // nil = лимит выключен (напр. в тестах)
}

// New собирает сервер со всеми зависимостями. limiter может быть nil —
// тогда rate limit на POST /events не навешивается.
func New(addr string, log *slog.Logger, repo *storage.Repo, channels []notify.Channel, limiter *ratelimit.Limiter) *Server {
	return &Server{addr: addr, log: log, repo: repo, channels: channels, limiter: limiter}
}

// routes возвращает обработчики.
//
// Два mux: прикладные маршруты (app) оборачиваем в withMetrics, а /metrics
// монтируем в root БЕЗ обёртки — иначе сам опрос метрик попадал бы в счётчики.
func (s *Server) routes() http.Handler {
	app := http.NewServeMux()
	app.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// POST /events — под пер-юзер rate limit (когда лимитер есть). Оборачиваем
	// только этот маршрут: /healthz и /metrics лимитировать не нужно.
	var createEvent http.Handler = http.HandlerFunc(s.handleCreateEvent)
	if s.limiter != nil {
		createEvent = s.withRateLimit(createEvent)
	}
	app.Handle("POST /events", createEvent)

	root := http.NewServeMux()
	// /metrics монтируем без withMetrics-обёртки: опрос метрик себя не считает.
	root.Handle("/metrics", promhttp.Handler())
	root.Handle("/", withMetrics(app))
	return root
}

// responseRecorder запоминает код ответа: http.ResponseWriter его не отдаёт,
// а для лейбла "status" метрики он нужен. Перехватываем WriteHeader.
// (Аналог в Rack — обёртка, читающая status из ответа middleware.)
type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// withMetrics — middleware: считает запросы и меряет длительность.
// r.Pattern (а не r.URL.Path) — шаблон маршрута ("POST /events"), чтобы лейбл
// status/path не плодил бесконечные значения (cardinality).
func withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		metrics.HTTPDuration.WithLabelValues(r.Pattern, r.Method).Observe(time.Since(start).Seconds())
		metrics.HTTPRequests.WithLabelValues(r.Pattern, r.Method, strconv.Itoa(rec.status)).Inc()
	})
}

// userKey определяет «пользователя» для rate limit: заголовок X-User-ID, а если
// его нет — IP клиента (чтобы анонимные клиенты тоже лимитировались). Префиксы
// "user:"/"ip:" разводят пространства ключей, чтобы id "1.2.3.4" из заголовка не
// совпал с IP-ключом.
func userKey(r *http.Request) string {
	if uid := r.Header.Get("X-User-ID"); uid != "" {
		return "ratelimit:user:" + uid
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr // без порта — берём как есть
	}
	return "ratelimit:ip:" + host
}

// withRateLimit — middleware пер-юзер лимита на POST /events. Спрашивает у
// s.limiter, можно ли пропустить запрос; при отказе отдаёт 429 с Retry-After.
// Ошибку лимитера не роняем наружу (fail-open): приём событий важнее лимита.
func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := userKey(r)
		allowed, retryAfter, err := s.limiter.Allow(r.Context(), key)
		if err != nil {
			s.log.Error("rate limit check failed", "error", err)
			next.ServeHTTP(w, r)
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retryAfter.Seconds()))))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// createEventRequest — тело POST /events.
type createEventRequest struct {
	EventID string          `json:"event_id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// channelNames — имена зарегистрированных каналов (для IngestEvent).
func (s *Server) channelNames() []string {
	names := make([]string, len(s.channels))
	for i, ch := range s.channels {
		names[i] = ch.Name()
	}
	return names
}

// handleCreateEvent принимает событие заказа и через IngestEvent атомарно
// сохраняет его и создаёт по одному pending-уведомлению на каждый канал.
// Повтор того же event_id идемпотентен: 200 duplicate, без новых уведомлений.
func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	var reg createEventRequest
	ctx := r.Context()
	if err := decodeJSON(r, &reg); err != nil {
		writeError(w, 400, "invalid request body")
		return
	}
	if reg.EventID == "" {
		writeError(w, 400, "event_id is required")
		return
	}
	if reg.Type == "" {
		writeError(w, 400, "type is required")
		return
	}
	ev, notes, created, err := s.repo.IngestEvent(ctx,
		storage.Event{ExternalID: reg.EventID, Type: reg.Type, Payload: reg.Payload},
		s.channelNames(),
	)
	if err != nil {
		writeError(w, 500, "failed to ingest event")
		s.log.Error("failed to ingest event", "err", err)
		return
	}
	if !created {
		metrics.EventsIngested.WithLabelValues("http", "duplicate").Inc()
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "event_id": reg.EventID})
		return
	}
	metrics.EventsIngested.WithLabelValues("http", "created").Inc()
	writeJSON(w, http.StatusCreated, map[string]any{"event": ev, "notifications": notes})
}

// Run запускает сервер и глушит его по отмене ctx (graceful shutdown).
func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:         s.addr,
		Handler:      s.routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	s.log.Info("listening", "addr", srv.Addr)
	select {
	case err := <-errCh: // сервер упал сам
		return err
	case <-ctx.Done(): // пришёл Ctrl+C — даём запросам доиграть, но не дольше 10с
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	}
}

// --- JSON-хелперы ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
