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

// testClient is created once and shared across all integration tests.
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

// consumeOne binds an exclusive queue to testExchange with the given routing
// key, drains one message within timeout, and returns the body. Used to verify
// that a task was actually published to the broker.
func consumeOne(t *testing.T, routingKey string, timeout time.Duration) []byte {
	t.Helper()
	conn, err := amqp.Dial(os.Getenv("TEST_AMQP_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })

	// Declare the exchange (idempotent — matches the client's declaration).
	require.NoError(t, ch.ExchangeDeclare(
		testExchange, "direct", true, false, false, false, nil,
	))

	// Declare a temporary exclusive queue and bind it.
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

// ---------------------------------------------------------------------------
// Execute
// ---------------------------------------------------------------------------

func TestClient_Execute(t *testing.T) {
	task := entity.ExecutionRecord{
		ExecutionID:     9001,
		CaseID:          1,
		Version:         vo.Version("v-exec"),
		ExecutionStatus: vo.StatusInit,
	}
	caseName := "ExecTestCase"

	// Start consuming BEFORE publishing so the message is not lost.
	bodyC := make(chan []byte, 1)
	go func() {
		bodyC <- consumeOne(t, string(task.Version), 5*time.Second)
	}()

	// Small delay to let the consumer binding settle.
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
// BatchRun — all tasks acked
// ---------------------------------------------------------------------------

func TestClient_BatchRun_AllSuccess(t *testing.T) {
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

// ---------------------------------------------------------------------------
// BatchRun — empty input
// ---------------------------------------------------------------------------

func TestClient_BatchRun_Empty(t *testing.T) {
	result, err := testClient.BatchRun(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, result.SuccessIDs)
	assert.Empty(t, result.FailedIDs)
}

// ---------------------------------------------------------------------------
// Close — idempotent
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
	// Second close must not panic or error.
	require.NoError(t, client.Close(ctx))
}
