// bench 是用于测量 ExecuteAllCases 下发耗时的轻量 gRPC 客户端。建议与
// execution-engine 部署在同一台机器上，使结果主要反映 DB 和 MQ 开销，
// 避免把额外网络 RTT 计入服务端性能。
//
// 用法：
//
//	./bench -addr localhost:9090 -version 26B -user bench \
//	  -request-id bench-20260720-01 -expect 52265 -json-out result.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "execution-engine/api/gen/taskexecution/v1"
)

type chunkFailure struct {
	Index   int32  `json:"index"`
	Message string `json:"message"`
}

type benchmarkReport struct {
	Status                string         `json:"status"`
	StartedAt             string         `json:"started_at"`
	FinishedAt            string         `json:"finished_at"`
	Address               string         `json:"address"`
	Version               string         `json:"version"`
	User                  string         `json:"user"`
	RequestID             string         `json:"request_id"`
	Expected              int            `json:"expected"`
	Planned               int32          `json:"planned"`
	Dispatched            int32          `json:"dispatched"`
	Failed                int            `json:"failed"`
	Unresolved            int            `json:"unresolved"`
	Chunks                int            `json:"chunks"`
	DurationMs            float64        `json:"duration_ms"`
	FirstProgressMs       float64        `json:"first_progress_ms"`
	P50ChunkIntervalMs    float64        `json:"p50_chunk_interval_ms"`
	P95ChunkIntervalMs    float64        `json:"p95_chunk_interval_ms"`
	ThroughputCasesPerSec float64        `json:"throughput_cases_per_sec"`
	ChunkErrors           []chunkFailure `json:"chunk_errors,omitempty"`
	StreamError           string         `json:"stream_error,omitempty"`
}

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC 服务地址")
	version := flag.String("version", "", "要下发的版本号（必填）")
	user := flag.String("user", "bench", "写入执行记录的操作人")
	requestID := flag.String("request-id", "", "本轮唯一的幂等请求标识（必填；重试时复用）")
	expected := flag.Int("expect", 0, "预期用例数；大于零时会校验 total_planned")
	timeout := flag.Duration("timeout", 10*time.Minute, "整次下发的超时时间")
	jsonOut := flag.String("json-out", "", "结果 JSON 文件；已存在时拒绝覆盖")
	flag.Parse()

	if *version == "" || *requestID == "" {
		fmt.Fprintln(os.Stderr, "错误：必须同时指定 -version 和 -request-id")
		flag.Usage()
		os.Exit(1)
	}

	conn, err := grpc.NewClient(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建到 %s 的连接失败：%v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := pb.NewTaskExecutionServiceClient(conn)

	fmt.Printf("连接 %s，下发 version=%q、user=%q、request_id=%q……\n", *addr, *version, *user, *requestID)

	startedAt := time.Now()
	stream, err := client.ExecuteAllCases(ctx, &pb.ExecuteAllCasesRequest{
		Version:   *version,
		User:      *user,
		RequestId: *requestID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "调用 ExecuteAllCases 失败：%v\n", err)
		os.Exit(1)
	}

	var (
		chunkCount      int
		totalDispatched int32
		totalPlanned    int32
		totalFailed     int
		firstProgress   time.Duration
		lastProgressAt  = startedAt
		intervals       []time.Duration
		chunkErrors     []chunkFailure
		streamErr       error
	)

	for {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			streamErr = recvErr
			break
		}

		now := time.Now()
		if chunkCount == 0 {
			firstProgress = now.Sub(startedAt)
		} else {
			intervals = append(intervals, now.Sub(lastProgressAt))
		}
		lastProgressAt = now

		chunkCount++
		totalDispatched = resp.TotalDispatched
		totalPlanned = resp.TotalPlanned
		totalFailed += len(resp.FailedExecutionIds)
		if resp.ChunkError != "" {
			chunkErrors = append(chunkErrors, chunkFailure{Index: resp.ChunkIndex, Message: resp.ChunkError})
			fmt.Printf("  分片 %d 异常：%s\n", resp.ChunkIndex, resp.ChunkError)
		}
	}

	finishedAt := time.Now()
	elapsed := finishedAt.Sub(startedAt)
	unresolved := int(totalPlanned) - int(totalDispatched) - totalFailed
	if unresolved < 0 {
		unresolved = 0
	}
	report := benchmarkReport{
		Status:             "PASS",
		StartedAt:          startedAt.UTC().Format(time.RFC3339Nano),
		FinishedAt:         finishedAt.UTC().Format(time.RFC3339Nano),
		Address:            *addr,
		Version:            *version,
		User:               *user,
		RequestID:          *requestID,
		Expected:           *expected,
		Planned:            totalPlanned,
		Dispatched:         totalDispatched,
		Failed:             totalFailed,
		Unresolved:         unresolved,
		Chunks:             chunkCount,
		DurationMs:         durationMs(elapsed),
		FirstProgressMs:    durationMs(firstProgress),
		P50ChunkIntervalMs: percentileMs(intervals, 0.50),
		P95ChunkIntervalMs: percentileMs(intervals, 0.95),
		ChunkErrors:        chunkErrors,
	}
	if elapsed > 0 {
		report.ThroughputCasesPerSec = float64(totalDispatched) / elapsed.Seconds()
	}
	if streamErr != nil {
		report.StreamError = streamErr.Error()
	}
	if streamErr != nil || totalFailed > 0 || unresolved > 0 || len(chunkErrors) > 0 || (*expected > 0 && int(totalPlanned) != *expected) {
		report.Status = "FAIL"
	}

	printReport(report)
	if *jsonOut != "" {
		if err := writeJSONReport(*jsonOut, report); err != nil {
			fmt.Fprintf(os.Stderr, "写入结果文件失败：%v\n", err)
			os.Exit(1)
		}
		fmt.Printf("结果文件:   %s\n", *jsonOut)
	}
	if report.Status != "PASS" {
		os.Exit(2)
	}
}

func durationMs(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func percentileMs(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*percentile)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return durationMs(ordered[index])
}

func printReport(report benchmarkReport) {
	fmt.Println("─────────────────────────────")
	fmt.Printf("结果:       %s\n", report.Status)
	fmt.Printf("耗时:       %.2f ms\n", report.DurationMs)
	fmt.Printf("首帧耗时:   %.2f ms\n", report.FirstProgressMs)
	fmt.Printf("总计划:     %d\n", report.Planned)
	fmt.Printf("成功下发:   %d\n", report.Dispatched)
	fmt.Printf("失败下发:   %d\n", report.Failed)
	fmt.Printf("未决数量:   %d\n", report.Unresolved)
	fmt.Printf("分片数:     %d\n", report.Chunks)
	fmt.Printf("平均速率:   %.0f 条/秒\n", report.ThroughputCasesPerSec)
	fmt.Printf("分片间隔:   P50 %.2f ms / P95 %.2f ms\n", report.P50ChunkIntervalMs, report.P95ChunkIntervalMs)
	fmt.Printf("请求标识:   %s\n", report.RequestID)
	if report.StreamError != "" {
		fmt.Printf("流式错误:   %s\n", report.StreamError)
	}
}

func writeJSONReport(path string, report benchmarkReport) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(report)
	closeErr := file.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}
