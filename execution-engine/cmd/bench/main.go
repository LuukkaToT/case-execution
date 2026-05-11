// bench is a lightweight gRPC client for measuring ExecuteAllCases dispatch
// latency. Run it on the same machine as the execution-engine server so that
// the measured time reflects only DB + MQ overhead, not network RTT.
//
// Usage:
//
//	./bench -addr localhost:9090 -version v1.0.0 -user bench
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "execution-engine/api/gen/taskexecution/v1"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "gRPC server address")
	version := flag.String("version", "", "version to dispatch (required)")
	user := flag.String("user", "bench", "operator name recorded on execution records")
	flag.Parse()

	if *version == "" {
		fmt.Fprintln(os.Stderr, "error: -version is required")
		flag.Usage()
		os.Exit(1)
	}

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewTaskExecutionServiceClient(conn)

	fmt.Printf("connecting to %s, dispatching version=%q user=%q ...\n", *addr, *version, *user)

	start := time.Now()
	stream, err := client.ExecuteAllCases(context.Background(),
		&pb.ExecuteAllCasesRequest{
			Version: *version,
			User:    *user,
		})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ExecuteAllCases: %v\n", err)
		os.Exit(1)
	}

	var (
		chunkCount      int
		totalDispatched int32
		totalPlanned    int32
		totalFailed     int
	)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "stream error: %v\n", err)
			os.Exit(1)
		}

		chunkCount++
		totalDispatched = resp.TotalDispatched
		totalPlanned = resp.TotalPlanned
		totalFailed += len(resp.FailedExecutionIds)

		if resp.ChunkError != "" {
			fmt.Printf("  chunk %d error: %s\n", resp.ChunkIndex, resp.ChunkError)
		}
	}

	elapsed := time.Since(start)

	fmt.Println("─────────────────────────────")
	fmt.Printf("耗时:       %v\n", elapsed)
	fmt.Printf("总计划:     %d\n", totalPlanned)
	fmt.Printf("成功下发:   %d\n", totalDispatched)
	fmt.Printf("失败下发:   %d\n", totalFailed)
	fmt.Printf("chunk 数:   %d\n", chunkCount)
	if totalPlanned > 0 {
		fmt.Printf("平均速率:   %.0f cases/s\n", float64(totalDispatched)/elapsed.Seconds())
	}
}

/*

在 Windows 上交叉编译出 Linux 二进制：

cd d:\Project\case-execution\execution-engine
$env:GOOS="linux"; $env:GOARCH="amd64"; go build -o bench.bin ./cmd/bench/
传到 Linux 执行机并运行：

ssh user@your-host "chmod +x /tmp/bench && /tmp/bench
参数	默认值	说明
-addr
localhost:9090
服务地址
-version
无，必填
要下发的版本号
-user
bench
记录在执行记录里的操作人



# 登上去跑
ssh user@192.168.x.x
chmod +x /tmp/bench
/tmp/bench -version 26B
*/
