// envinit 为测试环境补齐 MySQL 表结构，并初始化 RabbitMQ 交换机、队列和绑定。
// 它只执行非破坏性创建或迁移，不清表、不删表、不清空队列。
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"

	mysqlinfra "execution-engine/internal/infrastructure/persistence/mysql"
	"execution-engine/internal/infrastructure/persistence/mysql/po"
)

type initConfig struct {
	MySQL struct {
		DSN                string `mapstructure:"dsn"`
		MaxOpenConns       int    `mapstructure:"max_open_conns"`
		MaxIdleConns       int    `mapstructure:"max_idle_conns"`
		ConnMaxLifetimeSec int    `mapstructure:"conn_max_lifetime_sec"`
	} `mapstructure:"mysql"`
	RabbitMQ struct {
		URL      string `mapstructure:"url"`
		Exchange string `mapstructure:"exchange"`
	} `mapstructure:"rabbitmq"`
}

var safeIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func main() {
	configPath := flag.String("config", "", "引擎配置文件路径（必填）")
	queue := flag.String("queue", "", "要声明的持久化测试队列（必填）")
	routingKey := flag.String("routing-key", "", "要绑定的版本路由键（必填）")
	confirmTest := flag.Bool("confirm-test-environment", false, "确认目标是允许初始化的测试环境")
	allowSharedExchange := flag.Bool("allow-shared-exchange", false, "允许使用名称中不含 bench/test/qa/staging 的交换机")
	flag.Parse()

	if !*confirmTest {
		fatalf("拒绝执行：必须显式指定 -confirm-test-environment")
	}
	if *configPath == "" || *queue == "" || *routingKey == "" {
		fatalf("必须同时指定 -config、-queue 和 -routing-key")
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatalf("读取配置失败：%v", err)
	}
	if cfg.MySQL.DSN == "" || cfg.RabbitMQ.URL == "" || cfg.RabbitMQ.Exchange == "" {
		fatalf("配置必须包含 mysql.dsn、rabbitmq.url 和 rabbitmq.exchange")
	}
	if !*allowSharedExchange && !looksLikeTestResource(cfg.RabbitMQ.Exchange) {
		fatalf("交换机 %q 不像隔离测试资源；请改用含 bench/test/qa/staging 的名称，确需使用时显式指定 -allow-shared-exchange", cfg.RabbitMQ.Exchange)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	databaseName, created, err := ensureDatabase(ctx, cfg.MySQL.DSN)
	if err != nil {
		fatalf("初始化数据库失败：%v", err)
	}
	fmt.Printf("MySQL 数据库已就绪：%s（本次创建=%t）\n", databaseName, created)

	if err := migrateTables(cfg); err != nil {
		fatalf("补齐 MySQL 表结构失败：%v", err)
	}
	fmt.Println("MySQL 表已就绪：ut_case、execution_record、dispatch_outbox")

	messageCount, consumerCount, err := initializeRabbitMQ(cfg, *queue, *routingKey)
	if err != nil {
		fatalf("初始化 RabbitMQ 失败：%v", err)
	}
	fmt.Printf("RabbitMQ 已就绪：exchange=%s queue=%s routing_key=%s messages=%d consumers=%d\n",
		cfg.RabbitMQ.Exchange, *queue, *routingKey, messageCount, consumerCount)
	fmt.Println("初始化完成：没有清理任何旧数据，也没有向队列发布消息。")
}

func loadConfig(path string) (*initConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	var cfg initConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ensureDatabase(ctx context.Context, dsn string) (databaseName string, created bool, err error) {
	driverConfig, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", false, fmt.Errorf("解析 DSN：%w", err)
	}
	databaseName = driverConfig.DBName
	if databaseName == "" {
		return "", false, fmt.Errorf("DSN 未指定数据库名")
	}
	if !safeIdentifier.MatchString(databaseName) {
		return "", false, fmt.Errorf("数据库名 %q 包含不安全字符", databaseName)
	}

	adminConfig := *driverConfig
	adminConfig.DBName = ""
	adminDB, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		return "", false, err
	}
	defer adminDB.Close()
	if err := adminDB.PingContext(ctx); err != nil {
		return "", false, fmt.Errorf("连接 MySQL：%w", err)
	}

	var count int
	if err := adminDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", databaseName,
	).Scan(&count); err != nil {
		return "", false, fmt.Errorf("查询数据库是否存在：%w", err)
	}
	if count > 0 {
		return databaseName, false, nil
	}

	statement := fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", databaseName)
	if _, err := adminDB.ExecContext(ctx, statement); err != nil {
		return "", false, fmt.Errorf("创建数据库：%w", err)
	}
	return databaseName, true, nil
}

func migrateTables(cfg *initConfig) error {
	db, err := mysqlinfra.Open(mysqlinfra.Config{
		DSN:                cfg.MySQL.DSN,
		MaxOpenConns:       cfg.MySQL.MaxOpenConns,
		MaxIdleConns:       cfg.MySQL.MaxIdleConns,
		ConnMaxLifetimeSec: cfg.MySQL.ConnMaxLifetimeSec,
	})
	if err != nil {
		return err
	}
	defer mysqlinfra.Close(db)
	return db.AutoMigrate(&po.UtCase{}, &po.ExecutionRecord{}, &po.OutboxMessage{})
}

func initializeRabbitMQ(cfg *initConfig, queue, routingKey string) (messages, consumers int, err error) {
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		return 0, 0, fmt.Errorf("连接 RabbitMQ：%w", err)
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		return 0, 0, fmt.Errorf("创建 channel：%w", err)
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(cfg.RabbitMQ.Exchange, "direct", true, false, false, false, nil); err != nil {
		return 0, 0, fmt.Errorf("声明交换机：%w", err)
	}
	declared, err := channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("声明队列：%w", err)
	}
	if err := channel.QueueBind(declared.Name, routingKey, cfg.RabbitMQ.Exchange, false, nil); err != nil {
		return 0, 0, fmt.Errorf("绑定队列：%w", err)
	}
	inspected, err := channel.QueueInspect(declared.Name)
	if err != nil {
		return 0, 0, fmt.Errorf("检查队列：%w", err)
	}
	return inspected.Messages, inspected.Consumers, nil
}

func looksLikeTestResource(name string) bool {
	name = strings.ToLower(name)
	for _, marker := range []string{"bench", "test", "qa", "staging"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
