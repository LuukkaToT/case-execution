// Package rabbitmq 基于 amqp091-go 实现 port.UtCaseExecutorClient，并遵守
// amqp091 的并发约束：
//
//   - *amqp.Connection 支持并发使用，多个 goroutine 可同时调用 Channel()。
//   - *amqp.Channel 不支持并发使用，每个进行中的发布分片必须独占 channel。
//
// 客户端维护 channel_pool 个已开启 publisher confirm 的 *amqp.Channel；每次
// BatchRun 独占一个 channel。单连接上的多个 channel 由 amqp091 复用 TCP 帧，
// 实现分片级并行发布。
//
// # 高可用重连模型
//
// 后台 reconnectLoop 监听 conn.NotifyClose，连接断开时：
//
//  1. 把 connReady 替换为未关闭的阻塞 channel，使 borrow() 等待而不是立即失败。
//  2. 清空并关闭属于失效连接的空闲 channel。
//  3. 按可配置的指数退避策略重新连接。
//  4. 成功后补满 channel 池并关闭 connReady，唤醒全部等待者。
//
// 因此短暂故障期间，下发 worker 会等待连接恢复或自身 ctx 取消，而不是直接
// 收到 ErrNotConnected。
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/vo"
)

// Config 对应 configs/config.yaml 中的 rabbitmq 配置段。
type Config struct {
	URL              string
	Exchange         string
	ChannelPool      int
	PublisherBuffer  int
	ConfirmTimeoutMs int
	// ReconnectInitialBackoffMs 是首次重连前的等待时间，默认 1 秒。
	ReconnectInitialBackoffMs int
	// ReconnectMaxBackoffMs 是指数退避上限，默认 30 秒。
	ReconnectMaxBackoffMs int
}

func (c *Config) applyDefaults() {
	if c.ChannelPool <= 0 {
		c.ChannelPool = 8
	}
	if c.ConfirmTimeoutMs <= 0 {
		c.ConfirmTimeoutMs = 5000
	}
	if c.PublisherBuffer <= 0 {
		c.PublisherBuffer = 4096
	}
	if c.ReconnectInitialBackoffMs <= 0 {
		c.ReconnectInitialBackoffMs = 1000
	}
	if c.ReconnectMaxBackoffMs <= 0 {
		c.ReconnectMaxBackoffMs = 30000
	}
}

// 向上层返回的哨兵错误。
var (
	ErrClientClosed   = errors.New("rabbitmq client closed")
	ErrConfirmTimeout = errors.New("publisher confirm timed out")
)

// pooledChannel 组合 amqp.Channel、确认流、退回流和关闭通知，使 worker 能够
// 感知分片执行过程中的 channel 异常。
type pooledChannel struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	returns  chan amqp.Return
	closed   chan *amqp.Error
	// broken 表示 channel 已不可复用；release() 会丢弃并尝试创建替代 channel。
	broken bool
}

// Client 是支持自恢复和并发调用的 RabbitMQ 发布器。
//
// 以下内部状态由 mu 保护：
//
//   - conn：当前 AMQP 连接，重连期间为 nil。
//   - pool：保存可用 pooledChannel 的缓冲 channel，容量等于 ChannelPool。
//   - connReady：连接信号，关闭表示可用，打开表示重连中。
//   - closed：Close() 调用后置为 true，使后台 goroutine 退出。
type Client struct {
	cfg            Config
	confirmTimeout time.Duration
	log            *zap.Logger

	mu        sync.Mutex
	conn      *amqp.Connection
	pool      chan *pooledChannel // fixed-capacity; never replaced
	connReady chan struct{}       // closed when connected; open (blocking) when reconnecting
	closed    bool

	// Close() 关闭 stopCh，用于停止重连循环。
	stopCh chan struct{}
}

var _ port.UtCaseExecutorClient = (*Client)(nil)

// NewClient 连接 broker、预热 channel 池、启动重连循环，并返回可用客户端。
func NewClient(cfg Config, log *zap.Logger) (*Client, error) {
	if log == nil {
		log = zap.NewNop()
	}
	cfg.applyDefaults()

	// 初始连接可用，因此 connReady 从关闭状态开始。
	ready := make(chan struct{})
	close(ready)

	c := &Client{
		cfg:            cfg,
		confirmTimeout: time.Duration(cfg.ConfirmTimeoutMs) * time.Millisecond,
		log:            log,
		pool:           make(chan *pooledChannel, cfg.ChannelPool),
		connReady:      ready,
		stopCh:         make(chan struct{}),
	}
	if err := c.dial(); err != nil {
		return nil, err
	}
	go c.reconnectLoop()
	return c, nil
}

// ---------------------------------------------------------------------------
// 连接生命周期
// ---------------------------------------------------------------------------

// dial 连接 broker、声明 exchange，并用已开启 confirm 的 channel 填满池。
// 失败时清理所有已创建资源；调用方不得持有 c.mu。
func (c *Client) dial() error {
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	if err := c.declareExchange(conn); err != nil {
		_ = conn.Close()
		return err
	}
	channels, err := c.buildChannels(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}

	c.mu.Lock()
	c.conn = conn
	for _, pc := range channels {
		// pool 只在此处和 release() 中写入，正常情况下不会溢出。
		select {
		case c.pool <- pc:
		default:
			_ = pc.ch.Close()
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) declareExchange(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open declare channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := ch.ExchangeDeclare(
		c.cfg.Exchange, "direct", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare exchange %q: %w", c.cfg.Exchange, err)
	}
	return nil
}

func (c *Client) buildChannels(conn *amqp.Connection) ([]*pooledChannel, error) {
	channels := make([]*pooledChannel, 0, c.cfg.ChannelPool)
	for i := 0; i < c.cfg.ChannelPool; i++ {
		pc, err := openPooledChannel(conn, c.cfg.PublisherBuffer)
		if err != nil {
			for _, q := range channels {
				_ = q.ch.Close()
			}
			return nil, fmt.Errorf("open channel %d/%d: %w", i+1, c.cfg.ChannelPool, err)
		}
		channels = append(channels, pc)
	}
	return channels, nil
}

func openPooledChannel(conn *amqp.Connection, eventBuffer int) (*pooledChannel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &pooledChannel{
		ch:       ch,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, eventBuffer)),
		returns:  ch.NotifyReturn(make(chan amqp.Return, eventBuffer)),
		closed:   ch.NotifyClose(make(chan *amqp.Error, 1)),
	}, nil
}

// ---------------------------------------------------------------------------
// 重连循环：在客户端生命周期内由后台 goroutine 持续运行
// ---------------------------------------------------------------------------

// reconnectLoop 监听当前连接，连接意外关闭时启动重连；客户端关闭并触发
// stopCh 后退出。
func (c *Client) reconnectLoop() {
	for {
		// 获取当前连接快照并监听其关闭事件。
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			// 正常流程不应出现，用于防御并发边界。
			select {
			case <-c.stopCh:
				return
			case <-time.After(time.Second):
				continue
			}
		}

		// 阻塞等待连接关闭或客户端停止。
		connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-c.stopCh:
			return
		case amqpErr, ok := <-connClose:
			// ok=false 表示通知 channel 被关闭，通常由连接正常关闭触发。
			if !ok || amqpErr == nil {
				// 判断是否为主动关闭。
				c.mu.Lock()
				isClosed := c.closed
				c.mu.Unlock()
				if isClosed {
					return
				}
				// 健康连接出现无错误关闭通知时，按意外断线处理并尝试重连。
			}
			if amqpErr != nil {
				c.log.Warn("amqp connection closed unexpectedly",
					zap.String("reason", amqpErr.Error()),
					zap.Int("code", amqpErr.Code))
			} else {
				c.log.Warn("amqp connection closed (no error), triggering reconnect")
			}
		}

		c.handleDisconnect()

		// 检查重连过程中客户端是否已停止。
		c.mu.Lock()
		isClosed := c.closed
		c.mu.Unlock()
		if isClosed {
			return
		}
		// 继续循环，为新连接注册 NotifyClose。
	}
}

// handleDisconnect 通知 borrow() 当前处于重连状态，清空旧 channel 池，并按
// 指数退避持续尝试，直到连接成功或客户端关闭。
func (c *Client) handleDisconnect() {
	// --- 步骤一：通知全部 borrow() 调用方进入重连状态 ----------------
	newReady := make(chan struct{}) // 保持打开状态，使借用方在重连完成前阻塞。
	c.mu.Lock()
	c.conn = nil
	c.connReady = newReady
	c.mu.Unlock()

	// --- 步骤二：清理池中的旧 channel ---------------------------------
	// 正在被 worker 使用的 channel 会被标记为 broken，并在 release() 时因
	// conn == nil 被丢弃；这里仅需清理池中的空闲 channel。
drainLoop:
	for {
		select {
		case pc := <-c.pool:
			_ = pc.ch.Close()
		default:
			break drainLoop
		}
	}

	// --- 步骤三：按指数退避重试连接 -----------------------------------
	backoff := time.Duration(c.cfg.ReconnectInitialBackoffMs) * time.Millisecond
	maxBackoff := time.Duration(c.cfg.ReconnectMaxBackoffMs) * time.Millisecond
	attempt := 0

	for {
		select {
		case <-c.stopCh:
			return
		case <-time.After(backoff):
		}

		attempt++
		c.log.Info("reconnecting to RabbitMQ",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff))

		if err := c.reconnect(newReady); err != nil {
			if errors.Is(err, ErrClientClosed) {
				return
			}
			c.log.Warn("reconnect attempt failed",
				zap.Int("attempt", attempt),
				zap.Error(err))
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		c.log.Info("reconnected to RabbitMQ", zap.Int("attempts", attempt))
		return
	}
}

// reconnect 执行一次连接尝试。成功后更新 c.conn、补满池并关闭 readySignal
// 唤醒等待中的 borrow()；失败时返回错误，不修改现有客户端状态。
func (c *Client) reconnect(readySignal chan struct{}) error {
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	if err := c.declareExchange(conn); err != nil {
		_ = conn.Close()
		return err
	}
	channels, err := c.buildChannels(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}

	// 先提交新连接并填满池，再发送就绪信号。顺序不能反转，否则 worker 被
	// 唤醒后可能读到空池。Close 可能在拨号期间发生，因此提交前必须再次校验状态。
	c.mu.Lock()
	if c.closed || c.connReady != readySignal {
		c.mu.Unlock()
		for _, pc := range channels {
			_ = pc.ch.Close()
		}
		_ = conn.Close()
		return ErrClientClosed
	}
	c.conn = conn
	for _, pc := range channels {
		select {
		case c.pool <- pc:
		default:
			_ = pc.ch.Close()
		}
	}
	c.mu.Unlock()

	// 广播连接已恢复，唤醒全部阻塞的 borrow() goroutine。
	close(readySignal)
	return nil
}

// ---------------------------------------------------------------------------
// channel 池的借用与归还
// ---------------------------------------------------------------------------

// borrow 取出一个 channel 供单个 goroutine 独占使用。重连期间会阻塞到连接
// 恢复或 ctx 取消，使 worker 在短暂 broker 故障时等待而非立即失败。
func (c *Client) borrow(ctx context.Context) (*pooledChannel, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClientClosed
		}
		ready := c.connReady
		c.mu.Unlock()

		// 阶段一：等待客户端连接可用。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.stopCh:
			return nil, ErrClientClosed
		case <-ready:
			// 连接已恢复，进入阶段二。
		}

		// 阶段二：从池中获取 channel。buildChannels 和池写入在 mu 内完成，且
		// 早于 readySignal 关闭，因此走到这里时池已经填充；select 同时监听
		// ctx 和 stopCh，处理取消与关闭边界。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.stopCh:
			return nil, ErrClientClosed
		case pc, ok := <-c.pool:
			if !ok {
				return nil, ErrClientClosed
			}
			return pc, nil
		}
	}
}

// release 把 pc 归还到池中。若分片执行期间 channel 损坏，则尝试新建 channel
// 补位；连接本身断开、正在重连时只丢弃损坏 channel，由重连循环统一补池。
func (c *Client) release(pc *pooledChannel) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = pc.ch.Close()
		return
	}
	if !pc.broken {
		select {
		case c.pool <- pc:
			c.mu.Unlock()
			return
		default:
			// 正常情况下池容量等于预热数量，不会走到这里；该分支防御重复归还。
			c.mu.Unlock()
			_ = pc.ch.Close()
			return
		}
	}

	// 损坏 channel：先在锁内取得连接快照，再在锁外执行可能阻塞的网络操作。
	conn := c.conn
	c.mu.Unlock()
	_ = pc.ch.Close()
	if conn == nil || conn.IsClosed() {
		// 连接已失效，重连循环会在恢复后补满池，此处不放入无效替代项。
		return
	}
	fresh, err := openPooledChannel(conn, c.cfg.PublisherBuffer)
	if err != nil {
		c.log.Warn("backfill pooled channel failed", zap.Error(err))
		// 连接看似可用却无法创建 channel 时主动触发重连，避免池永久缩容。
		c.mu.Lock()
		stillCurrent := !c.closed && c.conn == conn
		c.mu.Unlock()
		if stillCurrent {
			_ = conn.Close()
		}
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.conn != conn {
		_ = fresh.ch.Close()
		return
	}
	select {
	case c.pool <- fresh:
	default:
		// 创建替代项期间重连循环可能已补满池，此时丢弃以避免溢出。
		_ = fresh.ch.Close()
	}
}

// ---------------------------------------------------------------------------
// 发布接口
// ---------------------------------------------------------------------------

// publishPayload 是执行机消费的标准 JSON 结构，与原 Python 下发格式保持兼容。
type publishPayload struct {
	ExecutionID int64  `json:"execution_id"`
	CaseName    string `json:"case_name"`
	Version     string `json:"version"`
}

// Execute 发布单条任务，内部复用 BatchRun，只维护一套确认和超时逻辑。
func (c *Client) Execute(ctx context.Context, record *entity.ExecutionRecord, caseName string) error {
	task := entity.ExecutionTask{
		ExecutionID: record.ExecutionID,
		CaseID:      record.CaseID,
		CaseName:    caseName,
		Version:     record.Version,
	}
	result, err := c.BatchRun(ctx, []entity.ExecutionTask{task})
	if err != nil {
		return err
	}
	if len(result.FailedIDs) > 0 {
		if result.ErrorMessage != "" {
			return errors.New(result.ErrorMessage)
		}
		return fmt.Errorf("execution_id=%d: publish nack'd", result.FailedIDs[0])
	}
	return nil
}

// BatchRun 在独占的 *amqp.Channel 上发布整个分片，并等待逐条 publisher
// confirm。同一分片不跨 channel，保证 seqNo 到 execution_id 的映射无歧义。
//
// 分片间并行由下发层的 N 个 worker 实现，每个 worker 调用 BatchRun 时借用
// 不同的池化 channel。
func (c *Client) BatchRun(ctx context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
	if len(tasks) == 0 {
		return vo.BatchResult{}, nil
	}
	pc, err := c.borrow(ctx)
	if err != nil {
		return failedAll(tasks, err.Error()), err
	}
	defer c.release(pc)

	// seq2id 把 AMQP delivery-tag 映射到 execution_id，用于确认结果回配。
	// amqp091 会在 Publish 内递增序号，因此必须在发布前读取 GetNextPublishSeqNo()。
	seq2id := make(map[uint64]int64, len(tasks))
	var publishErr error

	for _, t := range tasks {
		if err := ctx.Err(); err != nil {
			publishErr = err
			break
		}
		body, err := json.Marshal(publishPayload{
			ExecutionID: t.ExecutionID,
			CaseName:    t.CaseName,
			Version:     string(t.Version),
		})
		if err != nil {
			// 序列化失败是确定性错误，与 MQ 无关；标记当前任务失败并继续发布其余任务。
			c.log.Warn("marshal task payload failed",
				zap.Int64("execution_id", t.ExecutionID),
				zap.Error(err))
			continue
		}
		seq := pc.ch.GetNextPublishSeqNo()
		err = pc.ch.PublishWithContext(ctx,
			c.cfg.Exchange,
			string(t.Version), // routing key = version
			true, false,       // mandatory: an unroutable message is a dispatch failure
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    strconv.FormatInt(t.ExecutionID, 10),
				Type:         "ut_case.execute",
				Body:         body,
				Timestamp:    time.Now(),
			},
		)
		if err != nil {
			publishErr = err
			pc.broken = true
			break
		}
		seq2id[seq] = t.ExecutionID
	}

	// waitConfirms 会删除 seq2id 中已确认项，因此必须提前构建 publishedIDs，
	// 否则确认完成后会误把所有任务判为未发布。
	publishedIDs := make(map[int64]struct{}, len(seq2id))
	for _, id := range seq2id {
		publishedIDs[id] = struct{}{}
	}

	// 收集所有成功写入 channel 的发布确认。
	success, failed := c.waitConfirms(ctx, pc, seq2id)

	// 未进入 seq2id 的任务代表序列化失败或发布中断，必须归入失败结果。
	for _, t := range tasks {
		if _, ok := publishedIDs[t.ExecutionID]; !ok {
			failed = append(failed, t.ExecutionID)
		}
	}

	result := vo.BatchResult{SuccessIDs: success, FailedIDs: failed}
	if publishErr != nil {
		result.ErrorMessage = publishErr.Error()
		return result, publishErr
	}
	if len(failed) > 0 {
		result.ErrorMessage = "one or more messages were nack'd, returned, or unconfirmed"
	}
	return result, nil
}

// waitConfirms 持续读取确认，直到全部序号收到 ack/nack、确认超时、channel
// 关闭或 ctx 取消。所有未决序号归为失败，并把 channel 标记为 broken。
func (c *Client) waitConfirms(
	ctx context.Context,
	pc *pooledChannel,
	seq2id map[uint64]int64,
) (success, failed []int64) {
	if len(seq2id) == 0 {
		return
	}
	deadline := time.NewTimer(c.confirmTimeout)
	defer deadline.Stop()
	returned := make(map[int64]struct{})
	returnsCh := pc.returns

	for len(seq2id) > 0 {
		select {
		case <-ctx.Done():
			for _, id := range seq2id {
				failed = append(failed, id)
			}
			pc.broken = true
			return

		case <-deadline.C:
			for _, id := range seq2id {
				failed = append(failed, id)
			}
			pc.broken = true
			c.log.Warn("publisher confirm timeout",
				zap.Int("unconfirmed", len(seq2id)))
			return

		case reason, ok := <-pc.closed:
			// channel 在分片发布过程中关闭。
			for _, id := range seq2id {
				failed = append(failed, id)
			}
			pc.broken = true
			if ok && reason != nil {
				c.log.Warn("publisher channel closed mid-batch",
					zap.String("reason", reason.Error()))
			}
			return

		case returnedMessage, ok := <-returnsCh:
			if !ok {
				returnsCh = nil
				continue
			}
			if id, ok := executionIDFromReturn(returnedMessage); ok {
				returned[id] = struct{}{}
			}
			c.log.Warn("publisher message returned as unroutable",
				zap.String("message_id", returnedMessage.MessageId),
				zap.Uint16("reply_code", returnedMessage.ReplyCode),
				zap.String("reply_text", returnedMessage.ReplyText),
				zap.String("routing_key", returnedMessage.RoutingKey))

		case cnf, ok := <-pc.confirms:
			if !ok {
				for _, id := range seq2id {
					failed = append(failed, id)
				}
				pc.broken = true
				return
			}
			id, present := seq2id[cnf.DeliveryTag]
			if !present {
				// 当前 channel 上一轮发布残留的确认，可安全忽略。
				continue
			}
			delete(seq2id, cnf.DeliveryTag)
			if _, wasReturned := returned[id]; wasReturned {
				failed = append(failed, id)
			} else if cnf.Ack {
				success = append(success, id)
			} else {
				c.log.Warn("publisher nack received",
					zap.Int64("execution_id", id),
					zap.Uint64("delivery_tag", cnf.DeliveryTag))
				failed = append(failed, id)
			}
		}
	}

	// 对 mandatory 且无法路由的消息，RabbitMQ 会先发送 basic.return，再发送
	// basic.ack。库把两者投递到不同的缓冲 channel，select 可能任意选择，因此
	// 最后一条 confirm 后再次清空 return，并把误入成功分区的 ID 移到失败分区。
	for returnsCh != nil {
		select {
		case returnedMessage, ok := <-returnsCh:
			if !ok {
				returnsCh = nil
				continue
			}
			if id, ok := executionIDFromReturn(returnedMessage); ok {
				returned[id] = struct{}{}
			}
		default:
			returnsCh = nil
		}
	}
	if len(returned) == 0 {
		return
	}
	failedSet := make(map[int64]struct{}, len(failed)+len(returned))
	for _, id := range failed {
		failedSet[id] = struct{}{}
	}
	confirmed := success[:0]
	for _, id := range success {
		if _, wasReturned := returned[id]; wasReturned {
			if _, exists := failedSet[id]; !exists {
				failed = append(failed, id)
				failedSet[id] = struct{}{}
			}
			continue
		}
		confirmed = append(confirmed, id)
	}
	success = confirmed
	return
}

func executionIDFromReturn(returnedMessage amqp.Return) (int64, bool) {
	id, err := strconv.ParseInt(returnedMessage.MessageId, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// ---------------------------------------------------------------------------
// 关闭流程
// ---------------------------------------------------------------------------

// Close 停止重连循环、清空 channel 池并关闭底层 AMQP 连接。ctx 限制关闭总
// 时长，超时后直接关闭连接。
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	// 首先停止重连循环。
	close(c.stopCh)

	// 清空 channel 池。
	drained := 0
	poolCap := cap(c.pool)
drainClose:
	for drained < poolCap {
		select {
		case <-ctx.Done():
			break drainClose
		case pc, ok := <-c.pool:
			if !ok {
				break drainClose
			}
			_ = pc.ch.Close()
			drained++
		}
	}

	if conn != nil && !conn.IsClosed() {
		return conn.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

func failedAll(tasks []entity.ExecutionTask, msg string) vo.BatchResult {
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.ExecutionID)
	}
	return vo.BatchResult{FailedIDs: ids, ErrorMessage: msg}
}
