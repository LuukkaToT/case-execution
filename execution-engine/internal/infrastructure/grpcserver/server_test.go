package grpcserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer_HealthAndGracefulStop(t *testing.T) {
	handler := NewHandler(nil, zap.NewNop())
	srv, err := NewServer(Config{Addr: "127.0.0.1:0"}, handler, zap.NewNop())
	require.NoError(t, err)

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Start() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	srv.GracefulStop(stopCtx)
	require.NoError(t, <-serveDone)
}
