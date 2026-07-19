package grpcserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Config 对应 configs/config.yaml 中的 server 配置段。
type Config struct {
	Addr          string
	MaxRecvMsgMiB int
}

// Server 管理 grpc.Server 及其 listener 的完整生命周期。
type Server struct {
	cfg    Config
	log    *zap.Logger
	grpc   *grpc.Server
	health *health.Server
	lis    net.Listener
	handlr *Handler
}

// NewServer 创建带日志拦截器的 grpc.Server、绑定监听地址，并注册业务服务和
// 标准健康检查服务。这里不启动 goroutine，需要显式调用 Start。
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
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}
	return &Server{cfg: cfg, log: log, grpc: gs, health: healthServer, lis: lis, handlr: handler}, nil
}

// Addr 返回实际监听地址，cfg.Addr 使用 :0 时可据此获取动态端口。
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Start 阻塞执行 grpc.Serve，优雅退出时返回 nil。
func (s *Server) Start() error {
	s.log.Info("grpc server listening", zap.String("addr", s.Addr()))
	if err := s.grpc.Serve(s.lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

// GracefulStop 尝试在 ctx 截止时间内优雅停止；超时后执行强制 Stop，避免
// 进程永久阻塞。
func (s *Server) GracefulStop(ctx context.Context) {
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
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

// unaryLoggingInterceptor 以 INFO 级别记录一元 RPC 方法、延迟和状态。
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

// streamLoggingInterceptor 记录流式 RPC 方法、持续时间和状态。
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
