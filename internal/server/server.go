// Package server — HTTP-сервер сервиса уведомлений.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Server оборачивает http.Server с маршрутами сервиса.
type Server struct {
	addr string
	log  *slog.Logger
}

// New собирает сервер. Маршруты навешиваются в routes().
func New(addr string, log *slog.Logger) *Server {
	return &Server{addr: addr, log: log}
}

// routes возвращает обработчики. Пока только healthcheck.
//
// TODO(1): собери http.ServeMux, повесь "GET /healthz" — пусть пишет 200
// и короткое тело, например `{"status":"ok"}` (не забудь Content-Type).
// Верни mux (он удовлетворяет http.Handler).
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}

// Run запускает сервер и глушит его по отмене ctx (graceful shutdown).
//
// TODO(2): ты это уже делал в 05-listings-api — повтори по памяти:
//   - srv := &http.Server{Addr: s.addr, Handler: s.routes(),
//     ReadTimeout и WriteTimeout выставь (5–10s), чтобы не висли соединения};
//   - errCh := make(chan error, 1);
//   - go func() { логни "listening"; err := srv.ListenAndServe();
//     errors.Is(err, http.ErrServerClosed) — это НЕ ошибка, обычное закрытие };
//   - select: <-ctx.Done() -> graceful shutdown; errCh -> вернуть ошибку старта;
//   - на shutdown: ctx, cancel := context.WithTimeout(context.Background(), 10s);
//     defer cancel(); return srv.Shutdown(ctx).
func (s *Server) Run(ctx context.Context) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

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

	logger.Info("listening", "addr", srv.Addr)
	select {
	case err := <-errCh: // сервер упал сам
		return err
	case <-ctx.Done(): // пришёл Ctrl+C — даём запросам доиграть, но не дольше 10с
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	}
}
