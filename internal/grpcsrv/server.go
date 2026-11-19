// Package grpcsrv — gRPC-сервер статусов уведомлений.
package grpcsrv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	notificationv1 "github.com/ilyashilyaev/notification-service/internal/pb/notificationv1"
	"github.com/ilyashilyaev/notification-service/internal/storage"
)

// Server реализует gRPC NotificationService поверх репозитория.
// Встраивание UnimplementedNotificationServiceServer — требование grpc: даёт
// forward-compatible заглушки для методов, которые мы ещё не реализовали.
type Server struct {
	notificationv1.UnimplementedNotificationServiceServer
	addr string
	repo *storage.Repo
	log  *slog.Logger
}

func New(addr string, repo *storage.Repo, log *slog.Logger) *Server {
	return &Server{addr: addr, repo: repo, log: log}
}

// GetNotificationStatus возвращает статус уведомления по id.
func (s *Server) GetNotificationStatus(
	ctx context.Context,
	req *notificationv1.GetNotificationStatusRequest,
) (*notificationv1.GetNotificationStatusResponse, error) {
	n, err := s.repo.GetNotification(ctx, req.GetId())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "notification %d not found", req.GetId())
	}
	if err != nil {
		s.log.Error("GetNotificationStatus", "err", err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}
	return &notificationv1.GetNotificationStatusResponse{
		Id: n.ID, EventId: n.EventID, Channel: n.Channel,
		Status: string(n.Status), Attempts: int32(n.Attempts), LastError: n.LastError,
	}, nil
}

// Run поднимает gRPC-сервер и глушит его по отмене ctx (GracefulStop — доигрывает
// текущие вызовы, новых не принимает), по аналогии с server.Run для HTTP.
func (s *Server) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	gs := grpc.NewServer()
	notificationv1.RegisterNotificationServiceServer(gs, s)

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("grpc listening", "addr", s.addr)
		if err := gs.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		gs.GracefulStop()
		return nil
	}
}
