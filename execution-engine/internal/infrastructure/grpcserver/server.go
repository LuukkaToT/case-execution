package grpcserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Config mirrors the server section of configs/config.yaml.
type Config struct {
	Addr          string
	MaxRecvMsgMiB int
}

// Server owns the grpc.Server lifecycle plus its listener.
type Server struct {
	cfg    Config
	log    *zap.Logger
	grpc   *grpc.Server
	lis    net.Listener
	handlr *Handler
}

// NewServer builds the grpc.Server with logging interceptors, binds the
// listener, and registers the TaskExecutionService handler. No goroutines
// are started here - call Start() explicitly.
func NewServer(cfg Config, handler *Handler, log *zap.Logger) (*Server, error) {
	if log == nil {
		log = zap.NewNop()
	}
	if cfg.MaxRecvMsgMiB <= 0 {
		cfg.MaxRecvMsgMiB = 16
	}
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgMiB * 1024 * 1024),
		grpc.ChainUnaryInterceptor(unaryLoggingInterceptor(log)),
		grpc.ChainStreamInterceptor(streamLoggingInterceptor(log)),
	}
	gs := grpc.NewServer(opts...)
	handler.Register(gs)

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	return &Server{cfg: cfg, log: log, grpc: gs, lis: lis, handlr: handler}, nil
}

// Addr returns the bound address (useful when cfg.Addr uses :0).
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Start blocks on grpc.Serve; return nil on graceful stop.
func (s *Server) Start() error {
	s.log.Info("grpc server listening", zap.String("addr", s.Addr()))
	if err := s.grpc.Serve(s.lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

// GracefulStop tries to stop within ctx's deadline; if ctx expires we fall
// back to a hard Stop so shutdown cannot hang the process forever.
func (s *Server) GracefulStop(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-ctx.Done():
		s.log.Warn("grpc graceful stop exceeded deadline, forcing Stop")
		s.grpc.Stop()
		<-done
	case <-done:
	}
}

// unaryLoggingInterceptor logs method + latency + gRPC code at INFO.
func unaryLoggingInterceptor(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		log.Info("grpc unary",
			zap.String("method", info.FullMethod),
			zap.Duration("took", time.Since(start)),
			zap.Error(err),
		)
		return resp, err
	}
}

// streamLoggingInterceptor logs method + stream duration + gRPC code.
func streamLoggingInterceptor(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		log.Info("grpc stream",
			zap.String("method", info.FullMethod),
			zap.Duration("took", time.Since(start)),
			zap.Error(err),
		)
		return err
	}
}
