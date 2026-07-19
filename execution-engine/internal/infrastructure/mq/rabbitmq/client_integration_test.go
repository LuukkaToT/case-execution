//go:build integration

package rabbitmq

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"execution-engine/internal/domain/entity"
	"execution-engine/internal/domain/vo"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testExchange = "ut.exec.test"

// testClient 只创建一次，由本包全部集成测试共享。
var testClient *Client

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_AMQP_URL")
	if url == "" {
		os.Exit(0)
	}

	cfg := Config{
		URL:              url,
		Exchange:         testExchange,
		ChannelPool:      2,
		ConfirmTimeoutMs: 3000,
	}
	log, _ := zap.NewDevelopment()
	client, err := NewClient(cfg, log)
	if err != nil {
		panic("create test rabbitmq client: " + err.Error())
	}
	testClient = client
	code := m.Run()
	_ = testClient.Close(context.Background())
	os.Exit(code)
}

// consumeOne 使用指定路由键将独占队列绑定到 testExchange，
// 并在超时前消费一条消息、返回消息体，用于验证任务确实已发布到消息代理。
func consumeOne(t *testing.T, routingKey string, timeout time.Duration) []byte {
	t.Helper()
	conn, err := amqp.Dial(os.Getenv("TEST_AMQP_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	// 声明交换机；该操作幂等，并与客户端的声明保持一致。
	require.NoError(t, ch.ExchangeDeclare(
		testExchange, "direct", true, false, false, false, nil,
	))

	// 声明并绑定一个临时独占队列。
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, routingKey, testExchange, false, nil))

	deliveries, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	require.NoError(t, err)

	select {
	case d := <-deliveries:
		return d.Body
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message on queue")
		return nil
	}
}

// bindRoute 在测试生命周期内保持一个临时队列处于绑定状态。
// 发布确认 ACK 只代表交换机接收了消息；启用 mandatory 发布后，还要求真实路由存在。
func bindRoute(t *testing.T, routingKey string) {
	t.Helper()
	conn, err := amqp.Dial(os.Getenv("TEST_AMQP_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	require.NoError(t, ch.ExchangeDeclare(
		testExchange, "direct", true, false, false, false, nil,
	))
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(q.Name, routingKey, testExchange, false, nil))
}

// ---------------------------------------------------------------------------
// 单条下发
// ---------------------------------------------------------------------------

func TestClient_Execute(t *testing.T) {
	task := entity.ExecutionRecord{
		ExecutionID:     9001,
		CaseID:          1,
		Version:         vo.Version("v-exec"),
		ExecutionStatus: vo.StatusInit,
	}
	caseName := "ExecTestCase"

	// 在发布前启动消费者，避免消息因临时队列尚未绑定而丢失。
	bodyC := make(chan []byte, 1)
	go func() {
		bodyC <- consumeOne(t, string(task.Version), 5*time.Second)
	}()

	// 短暂等待，让消费者绑定完成。
	time.Sleep(100 * time.Millisecond)

	err := testClient.Execute(context.Background(), &task, caseName)
	require.NoError(t, err)

	body := <-bodyC
	var payload publishPayload
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, int64(9001), payload.ExecutionID)
	assert.Equal(t, caseName, payload.CaseName)
	assert.Equal(t, "v-exec", payload.Version)
}

// ---------------------------------------------------------------------------
// 批量下发：全部任务收到确认
// ---------------------------------------------------------------------------

func TestClient_BatchRun_AllSuccess(t *testing.T) {
	bindRoute(t, "v-batch")
	tasks := []entity.ExecutionTask{
		{ExecutionID: 9010, CaseID: 10, CaseName: "B1", Version: "v-batch"},
		{ExecutionID: 9011, CaseID: 11, CaseName: "B2", Version: "v-batch"},
		{ExecutionID: 9012, CaseID: 12, CaseName: "B3", Version: "v-batch"},
	}

	result, err := testClient.BatchRun(context.Background(), tasks)
	require.NoError(t, err)
	assert.Len(t, result.SuccessIDs, 3)
	assert.Empty(t, result.FailedIDs)
}

func TestClient_BatchRun_UnroutableIsFailed(t *testing.T) {
	tasks := []entity.ExecutionTask{
		{ExecutionID: 9020, CaseID: 20, CaseName: "NoRoute", Version: "v-no-route"},
	}

	result, err := testClient.BatchRun(context.Background(), tasks)
	require.NoError(t, err)
	assert.Empty(t, result.SuccessIDs)
	assert.Equal(t, []int64{9020}, result.FailedIDs)
	assert.NotEmpty(t, result.ErrorMessage)
}

// ---------------------------------------------------------------------------
// 批量下发：空输入
// ---------------------------------------------------------------------------

func TestClient_BatchRun_Empty(t *testing.T) {
	result, err := testClient.BatchRun(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, result.SuccessIDs)
	assert.Empty(t, result.FailedIDs)
}

// ---------------------------------------------------------------------------
// 关闭：幂等
// ---------------------------------------------------------------------------

func TestClient_Close_Idempotent(t *testing.T) {
	cfg := Config{
		URL:              os.Getenv("TEST_AMQP_URL"),
		Exchange:         testExchange,
		ChannelPool:      1,
		ConfirmTimeoutMs: 2000,
	}
	log, _ := zap.NewDevelopment()
	client, err := NewClient(cfg, log)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, client.Close(ctx))
	// 第二次关闭不能崩溃或返回错误。
	require.NoError(t, client.Close(ctx))
}
