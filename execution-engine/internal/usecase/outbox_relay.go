package usecase

import (
	"context"
	"fmt"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/vo"

	"go.uber.org/zap"
)

type OutboxRelayConfig struct {
	PollInterval   time.Duration // 轮询间隔：Run 每隔多久跑一次 DispatchOnce
	BatchSize      int           // 每轮最多认领多少条 PENDING 消息
	LeaseDuration  time.Duration // 认领租约时长；处理中占用这段时间，超时后可被其他实例重新认领
	MaxAttempts    int           // 单条消息最大重试次数；超过后标记为 DEAD / FAILED
	InitialBackoff time.Duration // 首次失败后的初始退避时间
	MaxBackoff     time.Duration // 指数退避上限，避免重试间隔无限变长
}

func (c OutboxRelayConfig) withDefaults() OutboxRelayConfig {
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = time.Minute
	}
	return c
}

// OutboxRelay 恢复因进程崩溃或发布后数据库事务失败而停留在 PENDING 的消息。
// ClaimPending 使用租约、租约令牌和 SKIP LOCKED，支持多个引擎实例并行执行。
type OutboxRelay struct {
	cfg      OutboxRelayConfig
	tx       TxRunner
	outbox   repository.OutboxRepository
	execRepo repository.ExecutionRepository
	executor port.UtCaseExecutorClient
	log      *zap.Logger
	now      func() time.Time
}

func NewOutboxRelay(
	cfg OutboxRelayConfig,
	tx TxRunner,
	outbox repository.OutboxRepository,
	execRepo repository.ExecutionRepository,
	executor port.UtCaseExecutorClient,
	log *zap.Logger,
) *OutboxRelay {
	if log == nil {
		log = zap.NewNop()
	}
	return &OutboxRelay{
		cfg:      cfg.withDefaults(),
		tx:       tx,
		outbox:   outbox,
		execRepo: execRepo,
		executor: executor,
		log:      log,
		now:      time.Now,
	}
}

// Run 持续轮询直到 ctx 取消。单轮失败只记录日志并在下一周期重试；持有者
// 崩溃后，超过租约时间的记录会重新变为可获取状态。
func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := r.DispatchOnce(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("outbox relay pass failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// DispatchOnce 最多获取并处理一个 relay 分片。该方法导出后可用于确定性测试
// 和运维场景下的单次修复命令。
func (r *OutboxRelay) DispatchOnce(ctx context.Context) error {
	now := r.now()
	messages, err := r.outbox.ClaimPending(ctx, r.cfg.BatchSize, now, now.Add(r.cfg.LeaseDuration))
	if err != nil {
		return fmt.Errorf("claim outbox: %w", err)
	}
	if len(messages) == 0 {
		return nil
	}
	leaseToken := messages[0].LeaseToken
	if leaseToken == "" {
		return fmt.Errorf("claimed outbox batch has an empty lease token")
	}
	for _, message := range messages[1:] {
		if message.LeaseToken != leaseToken {
			return fmt.Errorf("claimed outbox batch contains mixed lease tokens")
		}
	}

	tasks := make([]entity.ExecutionTask, 0, len(messages))
	records := make([]*entity.ExecutionRecord, 0, len(messages))
	messageByID := make(map[int64]*entity.OutboxMessage, len(messages))
	for _, message := range messages {
		tasks = append(tasks, message.Task())
		records = append(records, &entity.ExecutionRecord{
			ExecutionID:     message.ExecutionID,
			CaseID:          message.CaseID,
			Version:         message.Version,
			ExecutionStatus: vo.StatusInit,
		})
		messageByID[message.ExecutionID] = message
	}

	result, publishErr := r.executor.BatchRun(ctx, tasks)
	result = completeBatchResult(records, result)
	if publishErr != nil && result.ErrorMessage == "" {
		result.ErrorMessage = publishErr.Error()
	}
	if result.ErrorMessage == "" && len(result.FailedIDs) > 0 {
		result.ErrorMessage = "outbox publish was not confirmed"
	}

	successRecords := make([]*entity.ExecutionRecord, 0, len(result.SuccessIDs))
	for _, id := range result.SuccessIDs {
		message := messageByID[id]
		record := &entity.ExecutionRecord{ExecutionID: id, ExecutionStatus: vo.StatusInit}
		if message != nil {
			record.CaseID = message.CaseID
			record.Version = message.Version
		}
		if err := record.MarkAsWait(); err != nil {
			return fmt.Errorf("mark relayed execution %d WAIT: %w", id, err)
		}
		successRecords = append(successRecords, record)
	}

	var retryIDs, deadIDs []int64
	deadRecords := make([]*entity.ExecutionRecord, 0)
	maxAttempt := 1
	for _, id := range result.FailedIDs {
		message := messageByID[id]
		if message == nil {
			continue
		}
		if message.Attempts > maxAttempt {
			maxAttempt = message.Attempts
		}
		if message.Attempts < r.cfg.MaxAttempts {
			retryIDs = append(retryIDs, id)
			continue
		}
		record := &entity.ExecutionRecord{
			ExecutionID:     id,
			CaseID:          message.CaseID,
			Version:         message.Version,
			ExecutionStatus: vo.StatusInit,
		}
		if err := record.MarkAsFailed(); err != nil {
			return fmt.Errorf("mark exhausted execution %d FAILED: %w", id, err)
		}
		deadRecords = append(deadRecords, record)
		deadIDs = append(deadIDs, id)
	}

	allTerminalRecords := append(successRecords, deadRecords...)
	return r.tx.Do(ctx, func(ctx context.Context) error {
		if err := r.execRepo.BatchUpdateStatus(ctx, allTerminalRecords); err != nil {
			return err
		}
		if err := r.outbox.MarkPublished(ctx, result.SuccessIDs, leaseToken, now); err != nil {
			return err
		}
		if err := r.outbox.MarkDead(ctx, deadIDs, leaseToken, result.ErrorMessage); err != nil {
			return err
		}
		if len(retryIDs) > 0 {
			return r.outbox.MarkRetry(ctx, retryIDs, leaseToken, now.Add(r.retryBackoff(maxAttempt)), result.ErrorMessage)
		}
		return nil
	})
}

func (r *OutboxRelay) retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := r.cfg.InitialBackoff
	for i := 1; i < attempt && delay < r.cfg.MaxBackoff; i++ {
		delay *= 2
		if delay > r.cfg.MaxBackoff {
			delay = r.cfg.MaxBackoff
		}
	}
	return delay
}
