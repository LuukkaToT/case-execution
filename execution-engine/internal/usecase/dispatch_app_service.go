package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/service"
	"execution-engine/internal/domain/vo"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Config 控制下发流水线的并发和批量参数，零值或负值使用默认配置。
type Config struct {
	BatchSize            int           // 游标生产者的单分片大小，默认 1000
	WorkerCount          int           // 单条批量流水线的并发 worker 数，默认 4
	MaxConcurrentBatches int           // 全局最多并行执行的批量 RPC 数，默认 8
	OutboxFastPathGrace  time.Duration // relay 接管快速路径消息前的等待时间
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = 4
	}
	if c.MaxConcurrentBatches <= 0 {
		c.MaxConcurrentBatches = 8
	}
	if c.OutboxFastPathGrace <= 0 {
		c.OutboxFastPathGrace = 10 * time.Second
	}
	return c
}

// DispatchAppService 是三个下发接口对应的应用服务。它负责组织
// producer/worker/aggregator 流水线，但不依赖 gRPC；handler 层通过
// ProgressFn 将进度写回流式响应。
type DispatchAppService struct {
	cfg        Config
	tx         TxRunner
	caseRepo   repository.UtCaseRepository
	execRepo   repository.ExecutionRepository
	outboxRepo repository.OutboxRepository
	executor   port.UtCaseExecutorClient
	domainSvc  *service.ExecutionService
	log        *zap.Logger
	batchSlots chan struct{}
}

// WithOutbox 开启 DB 到 MQ 的可靠投递。Outbox 仅为单元测试和渐进式部署
// 保留可选能力，生产环境应始终注入该仓储。
func (s *DispatchAppService) WithOutbox(outbox repository.OutboxRepository) *DispatchAppService {
	s.outboxRepo = outbox
	return s
}

// NewDispatchAppService 创建应用服务并装配所需依赖。
func NewDispatchAppService(
	cfg Config,
	tx TxRunner,
	caseRepo repository.UtCaseRepository,
	execRepo repository.ExecutionRepository,
	executor port.UtCaseExecutorClient,
	log *zap.Logger,
) *DispatchAppService {
	if log == nil {
		log = zap.NewNop()
	}
	cfg = cfg.withDefaults()
	return &DispatchAppService{
		cfg:        cfg,
		tx:         tx,
		caseRepo:   caseRepo,
		execRepo:   execRepo,
		executor:   executor,
		domainSvc:  service.New(),
		log:        log,
		batchSlots: make(chan struct{}, cfg.MaxConcurrentBatches),
	}
}

// ---------------------------------------------------------------------------
// 单用例下发：三个阶段，对应原 Python ExecutionAppService.execute_case
// ---------------------------------------------------------------------------

// ExecuteCase 的执行步骤：
//  1. 事务内读取 UtCase、创建并保存 ExecutionRecord、回填主键。
//  2. 向 MQ 发布一条任务。
//  3. 事务内重新读取记录，根据 MQ 结果更新为 WAIT 或 FAILED。
//
// 返回执行记录 ID 和下发阶段状态，使 Python Web 无需再次查询。三个阶段均
// 响应 ctx；第二阶段完成后即使调用方取消，也会用独立补偿上下文尽力完成
// 第三阶段，避免记录长期停留在 INIT。
func (s *DispatchAppService) ExecuteCase(ctx context.Context, caseID int64, v vo.Version, user string) (int64, vo.ExecutionStatus, error) {
	return s.ExecuteCaseWithRequest(ctx, "", caseID, v, user)
}

func (s *DispatchAppService) ExecuteCaseWithRequest(
	ctx context.Context,
	requestID string,
	caseID int64,
	v vo.Version,
	user string,
) (int64, vo.ExecutionStatus, error) {
	if v.IsEmpty() {
		return 0, "", errs.NewInvalidArgument("version is required")
	}
	requestID, err := NormalizeRequestID(requestID)
	if err != nil {
		return 0, "", err
	}

	var (
		record   *entity.ExecutionRecord
		caseName string
	)

	// 阶段一：校验用例存在，并写入执行记录和 Outbox。
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
		ut, err := s.caseRepo.FindByID(ctx, caseID)
		if err != nil {
			return err
		}
		if ut == nil {
			return errs.NewCaseNotExist(caseID)
		}
		record = s.domainSvc.CreateExecutionRecordForRequest(requestID, caseID, v, user)
		caseName = ut.CaseName
		if err := s.execRepo.Add(ctx, record); err != nil {
			return err
		}
		if s.outboxRepo != nil {
			message := entity.NewOutboxMessage(record, caseName, time.Now().Add(s.cfg.OutboxFastPathGrace))
			if err := s.outboxRepo.Add(ctx, message); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, "", err
	}
	if record.Version != v {
		return record.ExecutionID, "", errs.NewInvalidArgument("request_id was already used with a different version")
	}

	// 🚩这里校验状态是为了防止，这是客户端的重试请求，即服务端执行完逻辑回复给客户端
	// 客户端没收到响应所以重试了。前面的Add方法会校验requestId，如果是重复的请求，会将
	// 之前的record写回， 这里通过判断状态就能确定是否为重试
	if record.ExecutionStatus != vo.StatusInit {
		if record.ExecutionStatus == vo.StatusFailed {
			return record.ExecutionID, vo.StatusFailed, nil
		}
		return record.ExecutionID, vo.StatusWait, nil
	}

	// 1. 🚩如果此处发生服务宕机mq还没发 , relay会再发一次, 这是异常情况的兜底

	// 阶段二：在数据库事务之外发布 MQ，与原 Python 语义一致。
	mqErr := s.executor.Execute(ctx, record, caseName)
	if mqErr != nil {
		s.log.Warn("mq publish failed for single case",
			zap.Int64("execution_id", record.ExecutionID),
			zap.Int64("case_id", caseID),
			zap.Error(mqErr))
	}

	// 2. 🚩如果此处发生服务宕机，Relay无法判断是否发过消息，会再发一次，所以这里的语义是 at-least-once
	// 至少会投递一次

	// 阶段三：根据发布结果更新状态。MQ 发布完成后，不能让 RPC 客户端断开
	// 阻止结果持久化，因此保留请求级 values，但为补偿写入提供独立超时。
	finalStatus := vo.StatusWait
	if mqErr != nil {
		finalStatus = vo.StatusFailed
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.tx.Do(writeCtx, func(ctx context.Context) error {
		cur, err := s.execRepo.FindByID(ctx, record.ExecutionID)
		if err != nil {
			return err
		}
		if cur == nil {
			return errs.NewExecutionNotFound(record.ExecutionID)
		}
		var transitErr error
		if mqErr != nil {
			transitErr = cur.MarkAsFailed()
		} else {
			transitErr = cur.MarkAsWait()
		}
		if transitErr != nil {
			return transitErr
		}
		if err := s.execRepo.Save(ctx, cur); err != nil {
			return err
		}
		if s.outboxRepo == nil {
			return nil
		}
		if mqErr != nil {
			return s.outboxRepo.MarkDead(ctx, []int64{record.ExecutionID}, "", mqErr.Error())
		}
		return s.outboxRepo.MarkPublished(ctx, []int64{record.ExecutionID}, "", time.Now())
	}); err != nil {
		return record.ExecutionID, finalStatus, err
	}

	return record.ExecutionID, finalStatus, nil
}

// ---------------------------------------------------------------------------
// 批量下发：游标生产者 -> channel -> N 个 worker -> 聚合器
// ---------------------------------------------------------------------------

// ExecuteAllCases 下发指定版本 v 绑定的全部用例。
func (s *DispatchAppService) ExecuteAllCases(ctx context.Context, v vo.Version, user string, cb ProgressFn) error {
	return s.ExecuteAllCasesWithRequest(ctx, "", v, user, cb)
}

func (s *DispatchAppService) ExecuteAllCasesWithRequest(ctx context.Context, requestID string, v vo.Version, user string, cb ProgressFn) error {
	if v.IsEmpty() {
		return errs.NewInvalidArgument("version is required")
	}
	requestID, err := NormalizeRequestID(requestID)
	if err != nil {
		return err
	}
	count := func(ctx context.Context) (int64, error) {
		return s.caseRepo.CountByVersion(ctx, v)
	}
	scan := func(ctx context.Context, size int, fn repository.UtCaseScanFn) error {
		return s.caseRepo.ScanByVersion(ctx, v, size, fn)
	}
	// 🚩 由于按信道执行和按照版本执行区别只在于筛选条件不同，下发流程一样，所以这里直接
	// 传入筛选条件，下发流程只负责调用，然后下发不是「为了函数指针而函数指针」，而是让
	// dispatchStream 只依赖「能 count、能 scan」这两个能力，和具体查询条件解耦。
	return s.dispatchStream(ctx, requestID, v, user, count, scan, cb)
}

// ExecuteChannelCases 下发指定渠道 ch 和版本 v 绑定的全部用例。
func (s *DispatchAppService) ExecuteChannelCases(ctx context.Context, ch vo.Channel, v vo.Version, user string, cb ProgressFn) error {
	return s.ExecuteChannelCasesWithRequest(ctx, "", ch, v, user, cb)
}

func (s *DispatchAppService) ExecuteChannelCasesWithRequest(ctx context.Context, requestID string, ch vo.Channel, v vo.Version, user string, cb ProgressFn) error {
	if v.IsEmpty() {
		return errs.NewInvalidArgument("version is required")
	}
	if ch.IsEmpty() {
		return errs.NewInvalidArgument("channel is required")
	}
	requestID, err := NormalizeRequestID(requestID)
	if err != nil {
		return err
	}
	count := func(ctx context.Context) (int64, error) {
		return s.caseRepo.CountByChannelVersion(ctx, ch, v)
	}
	scan := func(ctx context.Context, size int, fn repository.UtCaseScanFn) error {
		return s.caseRepo.ScanByChannelVersion(ctx, ch, v, size, fn)
	}
	return s.dispatchStream(ctx, requestID, v, user, count, scan, cb)
}

type countFn func(ctx context.Context) (int64, error)
type scanFn func(ctx context.Context, size int, fn repository.UtCaseScanFn) error

// indexedBatch 是生产者写入 batchCh 的数据，包含分片内容及单调递增编号。
// 编号由生产者分配，因此多个 worker 并发处理时 chunk_index 仍保持稳定。
type indexedBatch struct {
	Index int32
	Cases []*entity.UtCase
}

// dispatchStream 是两个批量 RPC 共用的生产消费流水线，重点处理 ctx 取消和
// channel 生命周期等边界情况。
func (s *DispatchAppService) dispatchStream(
	ctx context.Context,
	requestID string,
	v vo.Version,
	user string,
	count countFn,
	scan scanFn,
	cb ProgressFn,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.batchSlots <- struct{}{}:
		defer func() { <-s.batchSlots }()
	}

	total, err := count(ctx)
	if err != nil {
		return fmt.Errorf("count cases: %w", err)
	}
	if total == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)

	// batchCh 容量设为 worker 数的两倍，避免短时快速发布反向阻塞生产者；
	// 同时把峰值内存限制在约 WorkerCount*2*BatchSize 条用例。
	batchCh := make(chan indexedBatch, s.cfg.WorkerCount*2)
	progressCh := make(chan Progress, s.cfg.WorkerCount)

	// --- 生产者 ---------------------------------------------------------
	g.Go(func() error {
		defer close(batchCh)
		var idx int32
		err := scan(gctx, s.cfg.BatchSize, func(batch []*entity.UtCase) error {
			// 复制切片，避免底层分页实现复用 backing array 时与 worker 并发读取。
			cases := make([]*entity.UtCase, len(batch))
			copy(cases, batch) // 这里copy的效率？

			select {
			case <-gctx.Done():
				return gctx.Err()
			case batchCh <- indexedBatch{Index: idx, Cases: cases}:
				idx++
				return nil
			}
		})
		// errgroup 已把 ctx 取消作为主错误，此处不重复包装 ctx.Err()，只返回
		// 真正的扫描异常。
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("scan cases: %w", err)
		}
		return nil
	})

	// --- 并发 worker ----------------------------------------------------
	var wg sync.WaitGroup
	for i := 0; i < s.cfg.WorkerCount; i++ {
		wg.Add(1)
		workerID := i
		g.Go(func() error {
			defer wg.Done()
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					}
					if err := s.dispatchOneBatch(gctx, workerID, batch, requestID, v, user, progressCh); err != nil {
						return err
					}
				}
			}
		})
	}

	// --- 关闭器：全部 worker 结束后只关闭一次 progressCh -------------
	g.Go(func() error {
		wg.Wait()
		close(progressCh)
		return nil
	})

	// --- 聚合器：由单 goroutine 串行调用回调 ---------------------------
	g.Go(func() error {
		var totalDispatched int32
		for p := range progressCh {
			totalDispatched += int32(len(p.SuccessIDs))
			p.TotalDispatched = totalDispatched
			p.TotalPlanned = int32(total)
			if err := cb(p); err != nil {
				// 回调失败通常表示客户端断开、stream.Send 失败，必须取消整条流水线。
				return fmt.Errorf("progress callback: %w", err)
			}
		}
		return nil
	})

	return g.Wait()
}

// dispatchOneBatch 完成一个分片的三个 MySQL/MQ 阶段。只有需要放弃分片并
// 取消流水线的异常才返回 error；单条 MQ 失败通过 Progress.FailedIDs 返回。
func (s *DispatchAppService) dispatchOneBatch(
	ctx context.Context,
	workerID int,
	batch indexedBatch,
	requestID string,
	v vo.Version,
	user string,
	out chan<- Progress,
) error {
	chunkSize := int32(len(batch.Cases))
	records, nameMap := s.domainSvc.CreateBatchRecordsForRequest(requestID, batch.Cases, v, user)

	// 阶段一：批量插入执行记录和 Outbox，并回填主键。
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
		if err := s.execRepo.BatchAdd(ctx, records); err != nil {
			return err
		}
		if s.outboxRepo == nil {
			return nil
		}
		availableAt := time.Now().Add(s.cfg.OutboxFastPathGrace)
		messages := make([]*entity.OutboxMessage, 0, len(records))
		for _, record := range records {
			messages = append(messages, entity.NewOutboxMessage(record, nameMap[record.CaseID], availableAt))
		}
		return s.outboxRepo.BatchAdd(ctx, messages)
	}); err != nil {
		s.log.Error("batch insert failed, abandoning chunk",
			zap.Int("worker", workerID),
			zap.Int32("chunk_index", batch.Index),
			zap.Error(err))
		return s.emitProgress(ctx, out, Progress{
			ChunkIndex: batch.Index,
			ChunkSize:  chunkSize,
			ChunkError: "batch insert failed: " + err.Error(),
		})
	}

	// 阶段二：幂等重试复用已存在的记录。只有 INIT 记录需要发布；已经推进的
	// 记录直接写入进度结果，不重复发送消息。
	// 调用方带着同一个 request_id 又来了一次（网络重试、超时重打等）。
	// BatchAdd 因唯一键冲突回读已有记录，再按状态决定要不要重新发 MQ。
	// 🚩一句话：是可重入的幂等；INIT 会再发，已终态则只回放结果。重复投递窗口要靠消费端幂等兜住。
	// 网络断、客户端取消导致请求 ctx 取消、服务闪断后重启，只要调用方再用原来的 request_id，就能幂等重入。
	pendingRecords := make([]*entity.ExecutionRecord, 0, len(records))
	result := vo.BatchResult{}
	for _, record := range records {
		if record.Version != v {
			return s.emitProgress(ctx, out, Progress{
				ChunkIndex: batch.Index,
				ChunkSize:  chunkSize,
				ChunkError: "request_id was already used with a different version",
			})
		}
		switch record.ExecutionStatus {
		case vo.StatusInit:
			pendingRecords = append(pendingRecords, record)
		case vo.StatusFailed:
			result.FailedIDs = append(result.FailedIDs, record.ExecutionID)
		default:
			result.SuccessIDs = append(result.SuccessIDs, record.ExecutionID)
		}
	}

	newResult := vo.BatchResult{}
	var publishErr error
	if len(pendingRecords) > 0 {
		tasks := entity.BatchAssemble(pendingRecords, nameMap)
		newResult, publishErr = s.executor.BatchRun(ctx, tasks)
		newResult = completeBatchResult(pendingRecords, newResult)
	}
	if publishErr != nil {
		s.log.Warn("mq batch publish returned a partial or whole-batch error",
			zap.Int("worker", workerID),
			zap.Int32("chunk_index", batch.Index),
			zap.Error(publishErr))
		// BatchRun 在中途失败时会同时返回已确认分区和 error。必须保留已确认
		// 结果，只把未决记录归为失败，否则已经入队的任务会被误判为可重试，
		// 进而造成重复执行。
		if newResult.ErrorMessage == "" {
			newResult.ErrorMessage = publishErr.Error()
		}
	}
	s.domainSvc.DispatchBatch(pendingRecords, newResult)
	result.SuccessIDs = append(result.SuccessIDs, newResult.SuccessIDs...)
	result.FailedIDs = append(result.FailedIDs, newResult.FailedIDs...)
	result.ErrorMessage = newResult.ErrorMessage

	// 阶段三：使用独立补偿上下文持久化新状态。客户端取消 RPC 时 MQ 可能已经
	// 接收任务，继续使用已取消的请求上下文会使记录滞留在 INIT。
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	writeBackErr := s.tx.Do(writeCtx, func(ctx context.Context) error {
		if err := s.execRepo.BatchUpdateStatus(ctx, pendingRecords); err != nil {
			return err
		}
		if s.outboxRepo == nil {
			return nil
		}
		if err := s.outboxRepo.MarkPublished(ctx, newResult.SuccessIDs, "", time.Now()); err != nil {
			return err
		}
		return s.outboxRepo.MarkDead(ctx, newResult.FailedIDs, "", newResult.ErrorMessage)
	})
	if writeBackErr != nil {
		s.log.Error("batch status write-back failed",
			zap.Int("worker", workerID),
			zap.Int32("chunk_index", batch.Index),
			zap.Error(writeBackErr))
		result.ErrorMessage = appendErrorMessage(result.ErrorMessage, "status write-back failed: "+writeBackErr.Error())
	}

	return s.emitProgress(ctx, out, Progress{
		ChunkIndex: batch.Index,
		ChunkSize:  chunkSize,
		SuccessIDs: result.SuccessIDs,
		FailedIDs:  result.FailedIDs,
		ChunkError: result.ErrorMessage,
	})
}

func (s *DispatchAppService) emitProgress(ctx context.Context, out chan<- Progress, p Progress) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- p:
		return nil
	}
}

func extractExecutionIDs(records []*entity.ExecutionRecord) []int64 {
	out := make([]int64, 0, len(records))
	for _, r := range records {
		out = append(out, r.ExecutionID)
	}
	return out
}

// completeBatchResult 保证每条记录只出现在一个结果分区。显式失败优先于
// 成功，只把两个分区都缺失的 ID 归为失败，避免部分发布异常时把已确认消息
// 重新标记为可重试失败。
func completeBatchResult(records []*entity.ExecutionRecord, result vo.BatchResult) vo.BatchResult {
	failed := make(map[int64]struct{}, len(result.FailedIDs))
	for _, id := range result.FailedIDs {
		failed[id] = struct{}{}
	}
	success := make(map[int64]struct{}, len(result.SuccessIDs))
	for _, id := range result.SuccessIDs {
		if _, isFailed := failed[id]; !isFailed {
			success[id] = struct{}{}
		}
	}

	result.SuccessIDs = result.SuccessIDs[:0]
	result.FailedIDs = result.FailedIDs[:0]
	for _, record := range records {
		id := record.ExecutionID
		if _, ok := failed[id]; ok {
			result.FailedIDs = append(result.FailedIDs, id)
			continue
		}
		if _, ok := success[id]; ok {
			result.SuccessIDs = append(result.SuccessIDs, id)
			continue
		}
		result.FailedIDs = append(result.FailedIDs, id)
	}
	return result
}

func appendErrorMessage(current, next string) string {
	if current == "" {
		return next
	}
	return current + "; " + next
}

func NormalizeRequestID(requestID string) (string, error) {
	requestID = strings.TrimSpace(requestID)
	if len(requestID) > 64 {
		return "", errs.NewInvalidArgument("request_id must not exceed 64 bytes")
	}
	if requestID != "" {
		return requestID, nil
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate request_id: %w", err)
	}
	return hex.EncodeToString(random), nil
}
