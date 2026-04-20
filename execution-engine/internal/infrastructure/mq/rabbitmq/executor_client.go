// Package rabbitmq adapts port.UtCaseExecutorClient onto RabbitMQ via
// amqp091-go. The implementation respects the amqp091 threading contract:
//
//   - *amqp.Connection is safe for concurrent use (many goroutines may call
//     Channel() simultaneously).
//   - *amqp.Channel is NOT safe for concurrent use. Each in-flight publish
//     loop must own its channel for the duration of the batch.
//
// Concurrency is achieved by pooling `channel_pool` ready-to-publish
// *amqp.Channel instances (each with publisher confirms enabled) and letting
// dispatch workers check one out per BatchRun invocation. N Channels on a
// single Connection give true parallelism: amqp091 multiplexes TCP frames
// across channels.
//
// # High-availability reconnect model
//
// A background reconnectLoop goroutine watches conn.NotifyClose. On
// connection loss it:
//
//  1. Replaces connReady with a new open (blocking) channel so that borrow()
//     callers block rather than fail.
//  2. Drains and closes every pooled channel tied to the dead connection.
//  3. Retries dial with exponential back-off (configurable).
//  4. On success: pushes ChannelPool fresh channels into the pool and closes
//     connReady, broadcasting to all blocked borrow() callers.
//
// This means dispatch workers never see ErrNotConnected during a transient
// outage - they simply stall until the connection is restored or their ctx
// is cancelled.
package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/vo"
)

// Config mirrors the rabbitmq section of configs/config.yaml.
type Config struct {
	URL              string
	Exchange         string
	ChannelPool      int
	ConfirmTimeoutMs int
	// ReconnectInitialBackoffMs is the wait before the first retry (default 1 s).
	ReconnectInitialBackoffMs int
	// ReconnectMaxBackoffMs caps the exponential back-off (default 30 s).
	ReconnectMaxBackoffMs int
}

func (c *Config) applyDefaults() {
	if c.ChannelPool <= 0 {
		c.ChannelPool = 8
	}
	if c.ConfirmTimeoutMs <= 0 {
		c.ConfirmTimeoutMs = 5000
	}
	if c.ReconnectInitialBackoffMs <= 0 {
		c.ReconnectInitialBackoffMs = 1000
	}
	if c.ReconnectMaxBackoffMs <= 0 {
		c.ReconnectMaxBackoffMs = 30000
	}
}

// Sentinel errors surfaced to upper layers.
var (
	ErrClientClosed   = errors.New("rabbitmq client closed")
	ErrConfirmTimeout = errors.New("publisher confirm timed out")
)

// pooledChannel bundles an *amqp.Channel with its confirm stream and
// close-notifier so workers can react to mid-batch failures.
type pooledChannel struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
	closed   chan *amqp.Error
	// broken is set to true when the channel can no longer be reused;
	// release() will discard it and attempt to open a replacement.
	broken bool
}

// Client is a self-healing RabbitMQ publisher. Safe for concurrent use.
//
// Internal state (guarded by mu):
//
//   - conn      active AMQP connection (nil while reconnecting)
//   - pool      buffered channel of ready *pooledChannel; capacity == ChannelPool
//   - connReady signal channel; closed = connected/usable, open = reconnecting
//   - closed    set true by Close(), causes all goroutines to exit
type Client struct {
	cfg            Config
	confirmTimeout time.Duration
	log            *zap.Logger

	mu        sync.Mutex
	conn      *amqp.Connection
	pool      chan *pooledChannel // fixed-capacity; never replaced
	connReady chan struct{}        // closed when connected; open (blocking) when reconnecting
	closed    bool

	// stopCh is closed by Close() to stop the reconnect loop.
	stopCh chan struct{}
}

var _ port.UtCaseExecutorClient = (*Client)(nil)

// NewClient dials the broker, warms up the channel pool, starts the
// reconnect loop, and returns a ready-to-use client.
func NewClient(cfg Config, log *zap.Logger) (*Client, error) {
	if log == nil {
		log = zap.NewNop()
	}
	cfg.applyDefaults()

	// connReady starts CLOSED (we are connected).
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
// Connection lifecycle
// ---------------------------------------------------------------------------

// dial connects to the broker, declares the exchange, and fills the pool
// with confirm-enabled channels. On error every partial resource is cleaned.
// Caller must NOT hold c.mu.
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
		// pool is only written here and in release(); never overflows.
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
		pc, err := openPooledChannel(conn)
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

func openPooledChannel(conn *amqp.Connection) (*pooledChannel, error) {
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
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1024)),
		closed:   ch.NotifyClose(make(chan *amqp.Error, 1)),
	}, nil
}

// ---------------------------------------------------------------------------
// Reconnect loop (runs as a background goroutine for the client's lifetime)
// ---------------------------------------------------------------------------

// reconnectLoop watches the active connection and triggers a reconnect cycle
// whenever the connection is closed unexpectedly. It exits when stopCh is
// closed (i.e. when the client itself is closed).
func (c *Client) reconnectLoop() {
	for {
		// Obtain a snapshot of the current connection to watch.
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			// Shouldn't happen in normal operation; guard against races.
			select {
			case <-c.stopCh:
				return
			case <-time.After(time.Second):
				continue
			}
		}

		// Block until the connection is closed or the client stops.
		connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-c.stopCh:
			return
		case amqpErr, ok := <-connClose:
			// ok=false means the channel itself was garbage-collected or
			// the connection was closed cleanly (e.g. by our own Close()).
			if !ok || amqpErr == nil {
				// Check whether this is a deliberate shutdown.
				c.mu.Lock()
				isClosed := c.closed
				c.mu.Unlock()
				if isClosed {
					return
				}
				// Spurious close on an otherwise healthy conn - treat as
				// an unexpected drop and try to reconnect.
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

		// Check if we were stopped while reconnecting.
		c.mu.Lock()
		isClosed := c.closed
		c.mu.Unlock()
		if isClosed {
			return
		}
		// Loop to register a NotifyClose on the new connection.
	}
}

// handleDisconnect signals to borrow() callers that we are reconnecting,
// drains the stale pool, and then retries with exponential back-off until
// either the dial succeeds or the client is closed.
func (c *Client) handleDisconnect() {
	// --- Step 1: Signal "reconnecting" to all borrow() callers ----------
	newReady := make(chan struct{}) // intentionally left OPEN (blocking)
	c.mu.Lock()
	c.conn = nil
	c.connReady = newReady
	c.mu.Unlock()

	// --- Step 2: Drain stale channels from the pool ----------------------
	// Workers that currently hold a borrowed channel will mark it broken
	// and release() will discard it (conn == nil). We only need to drain
	// whatever is sitting idle in the pool.
drainLoop:
	for {
		select {
		case pc := <-c.pool:
			_ = pc.ch.Close()
		default:
			break drainLoop
		}
	}

	// --- Step 3: Retry with exponential back-off -------------------------
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

// reconnect performs a single dial attempt. On success it updates c.conn,
// refills the pool, and closes readySignal to unblock all waiting borrow()
// callers. On failure it returns an error without touching client state.
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

	// Commit the new connection and fill the pool before signalling.
	// Order matters: pool must be full before we close readySignal,
	// otherwise workers could rush borrow() and find an empty pool.
	c.mu.Lock()
	c.conn = conn
	for _, pc := range channels {
		select {
		case c.pool <- pc:
		default:
			_ = pc.ch.Close()
		}
	}
	c.mu.Unlock()

	// Broadcast "connected" to all blocked borrow() goroutines.
	close(readySignal)
	return nil
}

// ---------------------------------------------------------------------------
// Pool borrow / release
// ---------------------------------------------------------------------------

// borrow checks out a pooled channel for exclusive use by one goroutine.
// If the client is currently reconnecting, borrow blocks until the connection
// is restored or ctx is cancelled. This ensures workers stall rather than
// immediately fail during a transient broker outage.
func (c *Client) borrow(ctx context.Context) (*pooledChannel, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClientClosed
		}
		ready := c.connReady
		c.mu.Unlock()

		// Phase 1: wait until the client is connected.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.stopCh:
			return nil, ErrClientClosed
		case <-ready:
			// Connected; proceed to phase 2.
		}

		// Phase 2: grab a channel from the pool.
		// A very short window exists between connReady being closed and
		// the pool being populated, but buildChannels + pool push happens
		// under mu before close(readySignal), so the pool is always full
		// by the time we reach here. We use a select with ctx and stopCh
		// as a belt-and-suspenders guard.
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

// release returns pc to the pool. If pc is broken (mid-batch error), it
// opens a fresh channel as a replacement so the pool stays at full capacity.
// When the connection itself is dead (reconnect in progress), release just
// discards the broken channel; the reconnect loop is responsible for
// replenishing the pool.
func (c *Client) release(pc *pooledChannel) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		_ = pc.ch.Close()
		return
	}
	if !pc.broken {
		select {
		case c.pool <- pc:
			return
		default:
			// Should never happen (pool cap == warmup count), but guard
			// against double-release bugs.
			_ = pc.ch.Close()
			return
		}
	}

	// Broken channel: discard and attempt to open a replacement.
	_ = pc.ch.Close()
	if c.conn == nil || c.conn.IsClosed() {
		// Connection is dead; reconnect loop will refill the pool after
		// it reconnects. Do not push a nil/broken replacement.
		return
	}
	fresh, err := openPooledChannel(c.conn)
	if err != nil {
		c.log.Warn("backfill pooled channel failed", zap.Error(err))
		return
	}
	select {
	case c.pool <- fresh:
	default:
		// Pool already full (e.g. reconnect loop pushed channels while we
		// were building this replacement). Discard to avoid overflow.
		_ = fresh.ch.Close()
	}
}

// ---------------------------------------------------------------------------
// Publish API
// ---------------------------------------------------------------------------

// publishPayload is the canonical JSON structure consumed by workers.
// Kept byte-for-byte compatible with the Python dispatcher.
type publishPayload struct {
	ExecutionID int64  `json:"execution_id"`
	CaseName    string `json:"case_name"`
	Version     string `json:"version"`
}

// Execute publishes a single task. It internally delegates to BatchRun so
// there is only one confirm/timeout code path to maintain.
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

// BatchRun publishes every task on one exclusively owned *amqp.Channel and
// waits for per-task publisher confirms. The batch is NOT split across
// channels; all publishes and their confirms live on one channel so the
// seqNo → execution_id mapping stays unambiguous.
//
// Parallelism across batches comes from the dispatch layer: N worker
// goroutines each call BatchRun concurrently, each borrowing a different
// pooled channel.
func (c *Client) BatchRun(ctx context.Context, tasks []entity.ExecutionTask) (vo.BatchResult, error) {
	if len(tasks) == 0 {
		return vo.BatchResult{}, nil
	}
	pc, err := c.borrow(ctx)
	if err != nil {
		return failedAll(tasks, err.Error()), err
	}
	defer c.release(pc)

	// seq2id maps AMQP delivery-tag (sequence number) to execution_id so
	// we can match confirms back to tasks. We read GetNextPublishSeqNo()
	// BEFORE Publish because amqp091 increments the counter inside Publish.
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
			// Marshal failure is deterministic and not MQ-related; record
			// the task as failed and keep publishing the rest.
			c.log.Warn("marshal task payload failed",
				zap.Int64("execution_id", t.ExecutionID),
				zap.Error(err))
			continue
		}
		seq := pc.ch.GetNextPublishSeqNo()
		err = pc.ch.PublishWithContext(ctx,
			c.cfg.Exchange,
			string(t.Version), // routing key = version
			false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
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

	// Collect confirms for every successfully enqueued publish.
	success, failed := c.waitConfirms(ctx, pc, seq2id)

	// Tasks that never made it into seq2id (marshal error or publish abort)
	// must be flagged as failed in the result.
	publishedIDs := make(map[int64]struct{}, len(seq2id))
	for _, id := range seq2id {
		publishedIDs[id] = struct{}{}
	}
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
	return result, nil
}

// waitConfirms drains the confirms channel until all expected sequence
// numbers have been ack'd/nack'd, the confirm timeout fires, the channel
// dies, or ctx is cancelled. Any unresolved sequence is reported as failed
// and the channel is marked broken.
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
			// Channel died mid-batch.
			for _, id := range seq2id {
				failed = append(failed, id)
			}
			pc.broken = true
			if ok && reason != nil {
				c.log.Warn("publisher channel closed mid-batch",
					zap.String("reason", reason.Error()))
			}
			return

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
				// Stale confirm from a previous publish cycle on this
				// channel; safe to ignore.
				continue
			}
			delete(seq2id, cnf.DeliveryTag)
			if cnf.Ack {
				success = append(success, id)
			} else {
				c.log.Warn("publisher nack received",
					zap.Int64("execution_id", id),
					zap.Uint64("delivery_tag", cnf.DeliveryTag))
				failed = append(failed, id)
			}
		}
	}
	return
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Close stops the reconnect loop, drains the channel pool, and closes the
// underlying AMQP connection. ctx bounds the total time budget; if it
// expires the connection is force-closed.
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

	// Stop the reconnect loop first.
	close(c.stopCh)

	// Drain the pool.
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
// Helpers
// ---------------------------------------------------------------------------

func failedAll(tasks []entity.ExecutionTask, msg string) vo.BatchResult {
	ids := make([]int64, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.ExecutionID)
	}
	return vo.BatchResult{FailedIDs: ids, ErrorMessage: msg}
}
