// Package server — HTTP-сервер сервиса уведомлений.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/ilyashilyaev/notification-service/internal/notify"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Server оборачивает http.Server с маршрутами сервиса.
type Server struct {
	addr     string
	log      *slog.Logger
	repo     *storage.Repo
	channels []notify.Channel
}

// New собирает сервер со всеми зависимостями.
func New(addr string, log *slog.Logger, repo *storage.Repo, channels []notify.Channel) *Server {
	return &Server{addr: addr, log: log, repo: repo, channels: channels}
}

// routes возвращает обработчики.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /events", s.handleCreateEvent)
	return mux
}

// createEventRequest — тело POST /events.
type createEventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// handleCreateEvent принимает событие заказа, сохраняет его и создаёт
// по одному pending-уведомлению на каждый зарегистрированный канал.
func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	var reg createEventRequest
	ctx := r.Context()
	if err := decodeJSON(r, &reg); err != nil {
		writeError(w, 400, "invalid request body")
		return
	}
	if reg.Type == "" {
		writeError(w, 400, "type is required")
		return
	}
	ev, err := s.repo.CreateEvent(ctx, storage.Event{Type: reg.Type, Payload: reg.Payload})
	if err != nil {
		writeError(w, 500, "failed to create event")
		s.log.Error("failed to create event", "err", err)
		return
	}
	notes := make([]storage.Notification, 0, len(s.channels))
	for _, ch := range s.channels {
		n, err := s.repo.CreateNotification(ctx, storage.Notification{
			EventID: ev.ID,
			Channel: ch.Name(),
		})
		if err != nil {
			writeError(w, 500, "failed to create notification")
			return
		}
		notes = append(notes, n)
	}
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
