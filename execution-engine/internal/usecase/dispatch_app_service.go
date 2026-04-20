package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/errs"
	"execution-engine/internal/domain/port"
	"execution-engine/internal/domain/repository"
	"execution-engine/internal/domain/service"
	"execution-engine/internal/domain/vo"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Config tunes pipeline fan-out. Zero/negative values fall back to defaults.
type Config struct {
	BatchSize   int // per-chunk size for the cursor-style producer; default 1000
	WorkerCount int // concurrent dispatch workers; default 4
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = 4
	}
	return c
}

// DispatchAppService is the application service exposing the three dispatch
// RPC entry points. It owns the producer/workers/aggregator pipeline but
// does not know about gRPC - the handler layer injects a ProgressFn that
// writes to the stream.
type DispatchAppService struct {
	cfg       Config
	tx        TxRunner
	caseRepo  repository.UtCaseRepository
	execRepo  repository.ExecutionRepository
	executor  port.UtCaseExecutorClient
	domainSvc *service.ExecutionService
	log       *zap.Logger
}

// NewDispatchAppService wires the dispatch service with its collaborators.
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
	return &DispatchAppService{
		cfg:       cfg.withDefaults(),
		tx:        tx,
		caseRepo:  caseRepo,
		execRepo:  execRepo,
		executor:  executor,
		domainSvc: service.New(),
		log:       log,
	}
}

// ---------------------------------------------------------------------------
// Single-case dispatch (three-phase, matches ExecutionAppService.execute_case)
// ---------------------------------------------------------------------------

// ExecuteCase performs:
//  1. Tx: load UtCase, create ExecutionRecord, persist it, back-fill id.
//  2. Publish one task to the MQ.
//  3. Tx: re-read the record and mark it as WAIT / FAILED based on the MQ outcome.
//
// The function returns the execution id and the terminal-for-dispatch status,
// so the caller can propagate them to Python web without a second round trip.
// ctx is honoured across all three phases; cancellation after phase 2 still
// best-effort performs phase 3 under the parent ctx so the record does not
// remain stuck in StatusInit.
func (s *DispatchAppService) ExecuteCase(ctx context.Context, caseID int64, v vo.Version, user string) (int64, vo.ExecutionStatus, error) {
	if v.IsEmpty() {
		return 0, "", errs.NewInvalidArgument("version is required")
	}

	var (
		record   *entity.ExecutionRecord
		caseName string
	)

	// Phase 1: validate case existence + insert record.
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
		ut, err := s.caseRepo.FindByID(ctx, caseID)
		if err != nil {
			return err
		}
		if ut == nil {
			return errs.NewCaseNotExist(caseID)
		}
		record = s.domainSvc.CreateExecutionRecord(caseID, v, user)
		caseName = ut.CaseName
		return s.execRepo.Add(ctx, record)
	}); err != nil {
		return 0, "", err
	}

	// Phase 2: MQ publish (outside the DB transaction, matches Python).
	mqErr := s.executor.Execute(ctx, record, caseName)
	if mqErr != nil {
		s.log.Warn("mq publish failed for single case",
			zap.Int64("execution_id", record.ExecutionID),
			zap.Int64("case_id", caseID),
			zap.Error(mqErr))
	}

	// Phase 3: flip status based on publish outcome.
	finalStatus := vo.StatusWait
	if mqErr != nil {
		finalStatus = vo.StatusFailed
	}
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
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
		return s.execRepo.Save(ctx, cur)
	}); err != nil {
		return record.ExecutionID, finalStatus, err
	}

	return record.ExecutionID, finalStatus, nil
}

// ---------------------------------------------------------------------------
// Batch dispatch (cursor -> channel -> N workers -> aggregator)
// ---------------------------------------------------------------------------

// ExecuteAllCases dispatches every case bound to `v`.
func (s *DispatchAppService) ExecuteAllCases(ctx context.Context, v vo.Version, user string, cb ProgressFn) error {
	if v.IsEmpty() {
		return errs.NewInvalidArgument("version is required")
	}
	count := func(ctx context.Context) (int64, error) {
		return s.caseRepo.CountByVersion(ctx, v)
	}
	scan := func(ctx context.Context, size int, fn repository.UtCaseScanFn) error {
		return s.caseRepo.ScanByVersion(ctx, v, size, fn)
	}
	return s.dispatchStream(ctx, v, user, count, scan, cb)
}

// ExecuteChannelCases dispatches every case bound to (ch, v).
func (s *DispatchAppService) ExecuteChannelCases(ctx context.Context, ch vo.Channel, v vo.Version, user string, cb ProgressFn) error {
	if v.IsEmpty() {
		return errs.NewInvalidArgument("version is required")
	}
	if ch.IsEmpty() {
		return errs.NewInvalidArgument("channel is required")
	}
	count := func(ctx context.Context) (int64, error) {
		return s.caseRepo.CountByChannelVersion(ctx, ch, v)
	}
	scan := func(ctx context.Context, size int, fn repository.UtCaseScanFn) error {
		return s.caseRepo.ScanByChannelVersion(ctx, ch, v, size, fn)
	}
	return s.dispatchStream(ctx, v, user, count, scan, cb)
}

type countFn func(ctx context.Context) (int64, error)
type scanFn func(ctx context.Context, size int, fn repository.UtCaseScanFn) error

// indexedBatch is what the producer pushes onto batchCh: the chunk payload
// plus its monotonically increasing index (assigned producer-side so that
// chunk_index is stable across workers).
type indexedBatch struct {
	Index int32
	Cases []*entity.UtCase
}

// dispatchStream is the producer-consumer pipeline shared by the two batch
// RPCs. See the plan doc for the design; the comments below focus on the
// ctx / channel lifecycle corner-cases.
func (s *DispatchAppService) dispatchStream(
	ctx context.Context,
	v vo.Version,
	user string,
	count countFn,
	scan scanFn,
	cb ProgressFn,
) error {
	total, err := count(ctx)
	if err != nil {
		return fmt.Errorf("count cases: %w", err)
	}
	if total == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)

	// batchCh doubles the worker count so a burst of fast MQ publishes
	// cannot starve the producer; that bound also caps peak heap usage to
	// ~ (WorkerCount*2) * BatchSize cases.
	batchCh := make(chan indexedBatch, s.cfg.WorkerCount*2)
	progressCh := make(chan Progress, s.cfg.WorkerCount)

	// --- Producer ---------------------------------------------------------
	g.Go(func() error {
		defer close(batchCh)
		var idx int32
		err := scan(gctx, s.cfg.BatchSize, func(batch []*entity.UtCase) error {
			// Copy the slice: gorm.FindInBatches re-uses the same
			// backing array between callbacks, so handing it off to
			// a goroutine without copying would race.
			cases := make([]*entity.UtCase, len(batch))
			copy(cases, batch)

			select {
			case <-gctx.Done():
				return gctx.Err()
			case batchCh <- indexedBatch{Index: idx, Cases: cases}:
				idx++
				return nil
			}
		})
		// errgroup treats ctx cancellation as the dominant error; do not
		// surface ctx.Err() again here or callers will see a confusing
		// double-wrapped error. We only surface a real scan error.
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("scan cases: %w", err)
		}
		return nil
	})

	// --- Workers ----------------------------------------------------------
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
					if err := s.dispatchOneBatch(gctx, workerID, batch, v, user, progressCh); err != nil {
						return err
					}
				}
			}
		})
	}

	// --- Closer: shuts progressCh exactly once all workers have drained ---
	g.Go(func() error {
		wg.Wait()
		close(progressCh)
		return nil
	})

	// --- Aggregator: single goroutine owning the cb invocation ------------
	g.Go(func() error {
		var totalDispatched int32
		for p := range progressCh {
			totalDispatched += int32(len(p.SuccessIDs))
			p.TotalDispatched = totalDispatched
			p.TotalPlanned = int32(total)
			if err := cb(p); err != nil {
				// cb failure (typically stream.Send on a broken
				// client) must cancel the rest of the pipeline.
				return fmt.Errorf("progress callback: %w", err)
			}
		}
		return nil
	})

	return g.Wait()
}

// dispatchOneBatch performs the three MySQL+MQ phases for a single chunk.
// Returns a non-nil error only for "this batch lost, propagate and cancel
// the pipeline" conditions (DB write error, ctx cancellation). Per-task MQ
// failures are reported via Progress.FailedIDs and do NOT fail the batch.
func (s *DispatchAppService) dispatchOneBatch(
	ctx context.Context,
	workerID int,
	batch indexedBatch,
	v vo.Version,
	user string,
	out chan<- Progress,
) error {
	chunkSize := int32(len(batch.Cases))
	records, nameMap := s.domainSvc.CreateBatchRecords(batch.Cases, v, user)

	// Phase 1: insert records, back-fill ids.
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
		return s.execRepo.BatchAdd(ctx, records)
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

	// Phase 2: publish to MQ; wrap whole-batch errors into BatchResult so
	// phase 3 always runs and statuses match reality on disk.
	tasks := entity.BatchAssemble(records, nameMap)
	result, err := s.executor.BatchRun(ctx, tasks)
	if err != nil {
		s.log.Warn("mq batch publish failed, marking all as failed",
			zap.Int("worker", workerID),
			zap.Int32("chunk_index", batch.Index),
			zap.Error(err))
		result = vo.BatchResult{
			SuccessIDs:   nil,
			FailedIDs:    extractExecutionIDs(records),
			ErrorMessage: err.Error(),
		}
	}
	s.domainSvc.DispatchBatch(records, result)

	// Phase 3: persist new statuses. A failure here is logged but not
	// propagated: records carry their own id, MQ was already notified, and
	// a background reconciler (not in scope for this engine) can sweep up
	// the INIT stragglers; failing the RPC would double-send tasks on retry.
	if err := s.tx.Do(ctx, func(ctx context.Context) error {
		return s.execRepo.BatchUpdateStatus(ctx, records)
	}); err != nil {
		s.log.Error("batch status write-back failed",
			zap.Int("worker", workerID),
			zap.Int32("chunk_index", batch.Index),
			zap.Error(err))
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
