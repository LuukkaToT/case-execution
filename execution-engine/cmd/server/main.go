// execution-engine entry point: loads config, wires dependencies, runs the
// gRPC server until SIGINT/SIGTERM, then tears everything down in an order
// that matches the data flow:
//
//  1. Stop accepting new RPCs (grpc.GracefulStop) so in-flight dispatch
//     pipelines can finish cleanly under the parent ctx.
//  2. Close the MQ client: no more publishes will race with a disappearing
//     broker session.
//  3. Close the MySQL pool: last because repositories may be used by grpc
//     handlers finishing their write-back phase.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"execution-engine/internal/infrastructure/grpcserver"
	mqrmq "execution-engine/internal/infrastructure/mq/rabbitmq"
	mysqlinfra "execution-engine/internal/infrastructure/persistence/mysql"
	"execution-engine/internal/usecase"
	"execution-engine/pkg/logger"
)

// appConfig flattens the YAML into a single struct for easy wiring.
type appConfig struct {
	Server struct {
		GRPCAddr     string `mapstructure:"grpc_addr"`
		MaxRecvMsgMB int    `mapstructure:"max_recv_msg_mb"`
	} `mapstructure:"server"`

	MySQL struct {
		DSN                string `mapstructure:"dsn"`
		MaxOpenConns       int    `mapstructure:"max_open_conns"`
		MaxIdleConns       int    `mapstructure:"max_idle_conns"`
		ConnMaxLifetimeSec int    `mapstructure:"conn_max_lifetime_sec"`
	} `mapstructure:"mysql"`

	RabbitMQ struct {
		URL                       string `mapstructure:"url"`
		Exchange                  string `mapstructure:"exchange"`
		ChannelPool               int    `mapstructure:"channel_pool"`
		ConfirmTimeoutMs          int    `mapstructure:"confirm_timeout_ms"`
		ReconnectInitialBackoffMs int    `mapstructure:"reconnect_initial_backoff_ms"`
		ReconnectMaxBackoffMs     int    `mapstructure:"reconnect_max_backoff_ms"`
	} `mapstructure:"rabbitmq"`

	Dispatch struct {
		BatchSize   int `mapstructure:"batch_size"`
		WorkerCount int `mapstructure:"worker_count"`
	} `mapstructure:"dispatch"`

	Logger struct {
		Level    string `mapstructure:"level"`
		Encoding string `mapstructure:"encoding"`
	} `mapstructure:"logger"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := logger.Init(logger.Config{Level: cfg.Logger.Level, Encoding: cfg.Logger.Encoding}); err != nil {
		return fmt.Errorf("logger init: %w", err)
	}
	defer logger.Sync()
	log := logger.L()

	// --- Infrastructure ------------------------------------------------
	db, err := mysqlinfra.Open(mysqlinfra.Config{
		DSN:                cfg.MySQL.DSN,
		MaxOpenConns:       cfg.MySQL.MaxOpenConns,
		MaxIdleConns:       cfg.MySQL.MaxIdleConns,
		ConnMaxLifetimeSec: cfg.MySQL.ConnMaxLifetimeSec,
	})
	if err != nil {
		return fmt.Errorf("mysql open: %w", err)
	}
	defer func() { _ = mysqlinfra.Close(db) }()

	mqClient, err := mqrmq.NewClient(mqrmq.Config{
		URL:                       cfg.RabbitMQ.URL,
		Exchange:                  cfg.RabbitMQ.Exchange,
		ChannelPool:               cfg.RabbitMQ.ChannelPool,
		ConfirmTimeoutMs:          cfg.RabbitMQ.ConfirmTimeoutMs,
		ReconnectInitialBackoffMs: cfg.RabbitMQ.ReconnectInitialBackoffMs,
		ReconnectMaxBackoffMs:     cfg.RabbitMQ.ReconnectMaxBackoffMs,
	}, log.Named("rabbitmq"))
	if err != nil {
		return fmt.Errorf("rabbitmq init: %w", err)
	}
	// mqClient.Close is called explicitly during shutdown to keep the
	// ordering deterministic; no defer here.

	txRunner := mysqlinfra.NewTxRunner(db)
	execRepo := mysqlinfra.NewExecutionRepository(db)
	caseRepo := mysqlinfra.NewUtCaseRepository(db)

	// --- Application ---------------------------------------------------
	app := usecase.NewDispatchAppService(
		usecase.Config{
			BatchSize:   cfg.Dispatch.BatchSize,
			WorkerCount: cfg.Dispatch.WorkerCount,
		},
		txRunner, caseRepo, execRepo, mqClient,
		log.Named("usecase"),
	)

	// --- Transport -----------------------------------------------------
	handler := grpcserver.NewHandler(app, log.Named("grpc"))
	srv, err := grpcserver.NewServer(grpcserver.Config{
		Addr:          cfg.Server.GRPCAddr,
		MaxRecvMsgMiB: cfg.Server.MaxRecvMsgMB,
	}, handler, log.Named("grpc"))
	if err != nil {
		return fmt.Errorf("grpc server: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Start() }()

	// --- Wait for signal or fatal serve error --------------------------
	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("grpc serve exited with error", zap.Error(err))
		}
	case <-sigCtx.Done():
		log.Info("shutdown signal received, stopping")
	}

	// --- Graceful shutdown ---------------------------------------------
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv.GracefulStop(shutdownCtx)

	if err := mqClient.Close(shutdownCtx); err != nil {
		log.Warn("rabbitmq close error", zap.Error(err))
	}

	// Drain the serve channel (may be blocked if we came from a signal).
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("grpc serve exit", zap.Error(err))
		}
	case <-time.After(time.Second):
	}

	log.Info("execution-engine stopped")
	return nil
}

// loadConfig reads configs/config.yaml from cwd or the path given in
// EXEC_ENGINE_CONFIG. Env vars with prefix EXEC_ENGINE_ override any key
// via viper's AutomaticEnv + dot-to-underscore expansion.
func loadConfig() (*appConfig, error) {
	v := viper.New()
	if path := os.Getenv("EXEC_ENGINE_CONFIG"); path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath("./configs")
		v.AddConfigPath(".")
	}
	v.SetEnvPrefix("EXEC_ENGINE")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg appConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if cfg.Server.GRPCAddr == "" {
		cfg.Server.GRPCAddr = ":9090"
	}
	return &cfg, nil
}
