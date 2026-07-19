// execution-engine 入口：加载配置、装配依赖并启动 gRPC 服务，收到
// SIGINT/SIGTERM 后按数据流方向依次关闭：
//
//  1. 停止接收新 RPC（grpc.GracefulStop），等待正在执行的下发流水线结束。
//  2. 关闭 MQ 客户端，避免发布过程与连接销毁发生竞争。
//  3. 最后关闭 MySQL 连接池，因为收尾阶段的 gRPC handler 仍可能访问仓储。
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

// appConfig 将 YAML 配置映射成便于依赖装配的结构体。
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
		BatchSize            int `mapstructure:"batch_size"`
		WorkerCount          int `mapstructure:"worker_count"`
		MaxConcurrentBatches int `mapstructure:"max_concurrent_batches"`
	} `mapstructure:"dispatch"`

	Outbox struct {
		Enabled          bool `mapstructure:"enabled"`
		FastPathGraceMs  int  `mapstructure:"fast_path_grace_ms"`
		PollIntervalMs   int  `mapstructure:"poll_interval_ms"`
		BatchSize        int  `mapstructure:"batch_size"`
		LeaseDurationMs  int  `mapstructure:"lease_duration_ms"`
		MaxAttempts      int  `mapstructure:"max_attempts"`
		InitialBackoffMs int  `mapstructure:"initial_backoff_ms"`
		MaxBackoffMs     int  `mapstructure:"max_backoff_ms"`
	} `mapstructure:"outbox"`

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

	// --- 基础设施层 ----------------------------------------------------
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
		PublisherBuffer:           cfg.Dispatch.BatchSize,
		ConfirmTimeoutMs:          cfg.RabbitMQ.ConfirmTimeoutMs,
		ReconnectInitialBackoffMs: cfg.RabbitMQ.ReconnectInitialBackoffMs,
		ReconnectMaxBackoffMs:     cfg.RabbitMQ.ReconnectMaxBackoffMs,
	}, log.Named("rabbitmq"))
	if err != nil {
		return fmt.Errorf("rabbitmq init: %w", err)
	}
	// 正常退出时会按既定顺序显式关闭；此处的 defer 只负责覆盖后续装配失败等提前返回路径。
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mqClient.Close(closeCtx)
	}()

	txRunner := mysqlinfra.NewTxRunner(db)
	execRepo := mysqlinfra.NewExecutionRepository(db)
	caseRepo := mysqlinfra.NewUtCaseRepository(db)
	outboxRepo := mysqlinfra.NewOutboxRepository(db)

	// --- 应用层 --------------------------------------------------------
	app := usecase.NewDispatchAppService(
		usecase.Config{
			BatchSize:            cfg.Dispatch.BatchSize,
			WorkerCount:          cfg.Dispatch.WorkerCount,
			MaxConcurrentBatches: cfg.Dispatch.MaxConcurrentBatches,
			OutboxFastPathGrace:  time.Duration(cfg.Outbox.FastPathGraceMs) * time.Millisecond,
		},
		txRunner, caseRepo, execRepo, mqClient,
		log.Named("usecase"),
	)
	if cfg.Outbox.Enabled {
		app.WithOutbox(outboxRepo)
	}

	var relay *usecase.OutboxRelay
	if cfg.Outbox.Enabled {
		relay = usecase.NewOutboxRelay(usecase.OutboxRelayConfig{
			PollInterval:   time.Duration(cfg.Outbox.PollIntervalMs) * time.Millisecond,
			BatchSize:      cfg.Outbox.BatchSize,
			LeaseDuration:  time.Duration(cfg.Outbox.LeaseDurationMs) * time.Millisecond,
			MaxAttempts:    cfg.Outbox.MaxAttempts,
			InitialBackoff: time.Duration(cfg.Outbox.InitialBackoffMs) * time.Millisecond,
			MaxBackoff:     time.Duration(cfg.Outbox.MaxBackoffMs) * time.Millisecond,
		}, txRunner, outboxRepo, execRepo, mqClient, log.Named("outbox"))
	}

	// --- 传输层 --------------------------------------------------------
	handler := grpcserver.NewHandler(app, log.Named("grpc"))
	srv, err := grpcserver.NewServer(grpcserver.Config{
		Addr:          cfg.Server.GRPCAddr,
		MaxRecvMsgMiB: cfg.Server.MaxRecvMsgMB,
	}, handler, log.Named("grpc"))
	if err != nil {
		return fmt.Errorf("grpc server: %w", err)
	}

	// listener 创建成功后再启动后台 relay，避免后续装配失败留下孤儿 goroutine。
	var relayCancel context.CancelFunc
	var relayDone chan struct{}
	if relay != nil {
		var relayCtx context.Context
		relayCtx, relayCancel = context.WithCancel(context.Background())
		relayDone = make(chan struct{})
		go func() {
			defer close(relayDone)
			relay.Run(relayCtx)
		}()
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Start() }()

	// --- 等待退出信号或服务致命错误 ----------------------------------
	sigCtx, sigCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	var runErr error
	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("grpc serve exited with error", zap.Error(err))
			runErr = err
		}
	case <-sigCtx.Done():
		log.Info("shutdown signal received, stopping")
	}

	// --- 优雅关闭 ------------------------------------------------------
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv.GracefulStop(shutdownCtx)
	if relayCancel != nil {
		relayCancel()
		select {
		case <-relayDone:
		case <-shutdownCtx.Done():
			log.Warn("outbox relay shutdown exceeded deadline")
		}
	}

	if err := mqClient.Close(shutdownCtx); err != nil {
		log.Warn("rabbitmq close error", zap.Error(err))
	}

	// 消费 serve 结果；如果由系统信号触发退出，该 channel 可能仍在等待。
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("grpc serve exit", zap.Error(err))
		}
	case <-time.After(time.Second):
	}

	log.Info("execution-engine stopped")
	return runErr
}

// loadConfig 默认读取当前目录的 configs/config.yaml，也可以通过
// EXEC_ENGINE_CONFIG 指定路径。以 EXEC_ENGINE_ 开头的环境变量可借助
// Viper 的 AutomaticEnv 和点号转下划线规则覆盖配置项。
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
