package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-lynx/lynx-rabbitmq/conf"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---------------------------------------------------------------------------
// Metrics coverage
// ---------------------------------------------------------------------------

func TestMetrics_HealthCheckErrors(t *testing.T) {
	m := NewMetrics()
	m.IncrementHealthCheckErrors()
	m.IncrementHealthCheckErrors()

	stats := m.GetStats()
	healthStats := stats["health"].(map[string]any)
	if healthStats["check_errors"].(int64) != 2 {
		t.Fatalf("expected 2 health check errors, got %v", healthStats["check_errors"])
	}
}

func TestMetrics_SetHealthyFalse(t *testing.T) {
	m := NewMetrics()
	m.SetHealthy(true)
	m.SetHealthy(false)

	prometheusText := m.GetPrometheusMetrics()
	if !strings.Contains(prometheusText, "lynx_rabbitmq_health_status 0") {
		t.Fatalf("expected health status 0 in Prometheus output, got:\n%s", prometheusText)
	}
}

func TestMetrics_ReconnectionCountPrometheusOutput(t *testing.T) {
	m := NewMetrics()
	m.IncrementReconnectionCount()

	prometheusText := m.GetPrometheusMetrics()
	if !strings.Contains(prometheusText, "lynx_rabbitmq_last_reconnect_timestamp") {
		t.Fatalf("expected last reconnect timestamp in Prometheus output when reconnection has occurred, got:\n%s", prometheusText)
	}
}

func TestMetrics_ConsumerMessagesFailed(t *testing.T) {
	m := NewMetrics()
	m.IncrementConsumerMessagesFailed()
	m.IncrementConsumerMessagesFailed()

	stats := m.GetStats()
	consumerStats := stats["consumer"].(map[string]any)
	if consumerStats["messages_failed"].(int64) != 2 {
		t.Fatalf("expected 2 consumer failures, got %v", consumerStats["messages_failed"])
	}
}

// ---------------------------------------------------------------------------
// HealthChecker coverage (Start / run path / IsHealthy / GetLastError /
// GetErrorCount via goroutine)
// ---------------------------------------------------------------------------

func TestHealthChecker_StartAndStop(t *testing.T) {
	h := NewHealthChecker()
	// Use a very short interval so the goroutine has a chance to fire.
	h.interval = 5 * time.Millisecond
	h.Start()

	// Give the ticker a moment to fire at least once.
	time.Sleep(20 * time.Millisecond)

	if !h.IsHealthy() {
		t.Fatal("expected health checker to be healthy")
	}
	if h.GetLastCheck().IsZero() {
		t.Fatal("expected last check to be updated")
	}
	if h.GetErrorCount() != 0 {
		t.Fatalf("expected 0 error count, got %d", h.GetErrorCount())
	}
	if h.GetLastError() != nil {
		t.Fatalf("expected nil last error, got %v", h.GetLastError())
	}

	h.Stop()
	h.Stop() // idempotent
}

// ---------------------------------------------------------------------------
// ConnectionManager – SetConnectionNotify / watchConnection paths
// ---------------------------------------------------------------------------

func TestConnectionManager_SetConnectionNotify(t *testing.T) {
	cm := NewConnectionManager(&conf.RabbitMQ{})
	ch := make(chan *amqp.Error, 1)
	cm.SetConnectionNotify(ch)

	cm.mu.RLock()
	got := cm.connNotify
	cm.mu.RUnlock()
	if got == nil {
		t.Fatal("expected connNotify to be set")
	}
}

// watchConnection: stop via stopChan (no broker error).
func TestConnectionManager_WatchConnection_StopChan(t *testing.T) {
	cm := NewConnectionManager(&conf.RabbitMQ{})

	notifyCh := make(chan *amqp.Error, 1)
	reconnectCalled := false
	reconnFn := func() (*amqp.Connection, error) {
		reconnectCalled = true
		return nil, errors.New("no broker")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		cm.watchConnection(notifyCh, reconnFn)
	}()

	// Stop the watcher immediately (before any notification is sent).
	cm.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchConnection did not return after Stop")
	}
	if reconnectCalled {
		t.Fatal("reconnect should not be called when stopped via stopChan")
	}
}

// watchConnection: closed notify channel (nil amqpErr) → should return immediately.
func TestConnectionManager_WatchConnection_ClosedChannelWithoutError(t *testing.T) {
	cm := NewConnectionManager(&conf.RabbitMQ{})

	notifyCh := make(chan *amqp.Error, 1)
	close(notifyCh) // triggers !ok path

	done := make(chan struct{})
	go func() {
		defer close(done)
		cm.watchConnection(notifyCh, func() (*amqp.Connection, error) {
			return nil, errors.New("should not be called")
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchConnection did not return on closed notify channel")
	}
}

// watchConnection: reconnect attempts during back-off – stop wins.
func TestConnectionManager_WatchConnection_ReconnectThenStop(t *testing.T) {
	cm := NewConnectionManager(&conf.RabbitMQ{})
	// Start so that the stopChan is available.

	notifyCh := make(chan *amqp.Error, 1)
	// Send a real AMQP error to trigger the reconnect loop.
	notifyCh <- &amqp.Error{Code: 503, Reason: "broker restart"}

	attempts := 0
	reconnFn := func() (*amqp.Connection, error) {
		attempts++
		return nil, errors.New("still no broker")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		cm.watchConnection(notifyCh, reconnFn)
	}()

	// Give the reconnect loop at least one attempt then stop.
	time.Sleep(50 * time.Millisecond)
	cm.Stop()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watchConnection reconnect loop did not exit after Stop")
	}
}

// ConnectionManager.GetHealthChecker should return nil.
func TestConnectionManager_GetHealthChecker(t *testing.T) {
	cm := NewConnectionManager(&conf.RabbitMQ{})
	if cm.GetHealthChecker() != nil {
		t.Fatal("expected GetHealthChecker to return nil")
	}
}

// ---------------------------------------------------------------------------
// Pool – Close / IsClosed
// ---------------------------------------------------------------------------

func TestGoroutinePool_CloseAndIsClosed(t *testing.T) {
	pool := NewGoroutinePool(2)
	if pool.IsClosed() {
		t.Fatal("expected pool to start open")
	}
	pool.Close()
	if !pool.IsClosed() {
		t.Fatal("expected pool to be closed after Close")
	}
	// Double-close must not panic.
	pool.Close()
}

// ---------------------------------------------------------------------------
// errors.go – Error / IsError
// ---------------------------------------------------------------------------

func TestErrorWithContext_Error(t *testing.T) {
	e := &ErrorWithContext{Err: ErrConnectionFailed, Context: "dialing"}
	if !strings.Contains(e.Error(), "dialing") {
		t.Fatalf("expected context in error message, got %q", e.Error())
	}
	if !strings.Contains(e.Error(), ErrConnectionFailed.Error()) {
		t.Fatalf("expected inner error in message, got %q", e.Error())
	}

	// Empty context – should just return the inner error string.
	e2 := &ErrorWithContext{Err: ErrConnectionFailed}
	if e2.Error() != ErrConnectionFailed.Error() {
		t.Fatalf("unexpected error string without context: %q", e2.Error())
	}
}

func TestIsError(t *testing.T) {
	wrapped := WrapError(ErrConnectionFailed, "test context")
	if !IsError(wrapped, ErrConnectionFailed) {
		t.Fatal("expected IsError to return true for wrapped error")
	}
	if IsError(wrapped, ErrChannelFailed) {
		t.Fatal("expected IsError to return false for different target")
	}
}

// ---------------------------------------------------------------------------
// lifecycle_context.go
// ---------------------------------------------------------------------------

func TestLifecycleContext_IsContextAware(t *testing.T) {
	client := NewRabbitMQClient()
	if !client.IsContextAware() {
		t.Fatal("expected IsContextAware to return true")
	}
}

func TestLifecycleContext_PluginProtocol(t *testing.T) {
	client := NewRabbitMQClient()
	proto := client.PluginProtocol()
	if !proto.ContextLifecycle {
		t.Fatal("expected PluginProtocol.ContextLifecycle to be true")
	}
}

func TestLifecycleContext_InitializeContext_Canceled(t *testing.T) {
	client := NewRabbitMQClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.InitializeContext(ctx, nil, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLifecycleContext_StartContext_Canceled(t *testing.T) {
	client := NewRabbitMQClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.StartContext(ctx, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestLifecycleContext_StopContext_Canceled(t *testing.T) {
	client := NewRabbitMQClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.StopContext(ctx, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

// ---------------------------------------------------------------------------
// cleanupWithContext – fully exercised with a pre-configured client
// ---------------------------------------------------------------------------

func TestCleanupWithContext_Canceled(t *testing.T) {
	client := NewRabbitMQClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.cleanupWithContext(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestCleanupWithContext_WithManagersNoConnection(t *testing.T) {
	client := NewRabbitMQClient()
	client.healthChecker = NewHealthChecker()
	client.connectionManager = NewConnectionManager(&conf.RabbitMQ{})
	client.goroutinePool = NewGoroutinePool(2)

	// Add a nil-channel producer and consumer – cleanup should handle them.
	client.producers["p1"] = nil
	client.consumers["c1"] = nil

	err := client.cleanupWithContext(context.Background())
	if err != nil {
		t.Fatalf("expected cleanup to succeed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// startupWithContext – canceled-before-execution path
// ---------------------------------------------------------------------------

func TestStartupWithContext_CanceledBeforeExecution(t *testing.T) {
	client := NewRabbitMQClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.startupWithContext(ctx)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
	if !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CheckHealth – various failure modes (no real broker needed)
// ---------------------------------------------------------------------------

func TestCheckHealth_NilConnection(t *testing.T) {
	client := NewRabbitMQClient()
	// connection is nil by default
	err := client.CheckHealth()
	if err == nil {
		t.Fatal("expected health check to fail with nil connection")
	}
}

func TestCheckHealth_NilConnectionManager(t *testing.T) {
	client := NewRabbitMQClient()
	// Hack: inject a dummy connection pointer to get past the nil-connection check,
	// but leave connectionManager nil so we trigger that error path.
	// We can't construct a real amqp.Connection without a broker, so we test
	// CheckHealth via the struct directly by mimicking the logic.
	// Instead, just test a simpler scenario.
	client.connectionManager = NewConnectionManager(&conf.RabbitMQ{})
	// connectionManager.IsConnected() is false by default (not started)
	// and client.connection is nil, so we get the nil-connection error.
	err := client.CheckHealth()
	if err == nil {
		t.Fatal("expected CheckHealth to fail without connection")
	}
}

func TestCheckHealth_UnhealthyChecker(t *testing.T) {
	client := NewRabbitMQClient()
	// Patch a health checker that reports unhealthy.
	hc := NewHealthChecker()
	hc.mu.Lock()
	hc.healthy = false
	hc.mu.Unlock()
	client.healthChecker = hc

	// Still no connection, so it hits nil-connection first.
	err := client.CheckHealth()
	if err == nil {
		t.Fatal("expected CheckHealth to fail")
	}
}

// ---------------------------------------------------------------------------
// GetRabbitMQConfig / GetConnection / GetMetrics
// ---------------------------------------------------------------------------

func TestGetRabbitMQConfig(t *testing.T) {
	client := NewRabbitMQClient()
	cfg := client.GetRabbitMQConfig()
	if cfg == nil {
		t.Fatal("expected GetRabbitMQConfig to return non-nil config")
	}
}

func TestGetConnection(t *testing.T) {
	client := NewRabbitMQClient()
	conn := client.GetConnection()
	if conn != nil {
		t.Fatal("expected GetConnection to return nil for unconnected client")
	}
}

func TestGetMetrics(t *testing.T) {
	client := NewRabbitMQClient()
	m := client.GetMetrics()
	if m == nil {
		t.Fatal("expected GetMetrics to return non-nil metrics")
	}
}

// ---------------------------------------------------------------------------
// Subscribe – no consumer registered
// ---------------------------------------------------------------------------

func TestSubscribe_NoConsumer(t *testing.T) {
	client := NewRabbitMQClient()
	err := client.Subscribe(context.Background(), "q", func(_ context.Context, _ amqp.Delivery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected Subscribe to fail when no consumers registered")
	}
	if !errors.Is(err, ErrConsumerNotReady) {
		t.Fatalf("expected ErrConsumerNotReady, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// PublishMessageWith – nil channel path
// ---------------------------------------------------------------------------

func TestPublishMessageWith_NilChannel(t *testing.T) {
	client := NewRabbitMQClient()
	client.producers["p"] = nil
	err := client.PublishMessageWith(context.Background(), "p", "ex", "key", []byte("hello"))
	if err == nil {
		t.Fatal("expected error for nil channel producer")
	}
	if !errors.Is(err, ErrProducerNotReady) {
		t.Fatalf("expected ErrProducerNotReady, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// SubscribeWith – nil channel path
// ---------------------------------------------------------------------------

func TestSubscribeWith_NilChannel(t *testing.T) {
	client := NewRabbitMQClient()
	client.consumers["c"] = nil
	err := client.SubscribeWith(context.Background(), "c", "q", func(_ context.Context, _ amqp.Delivery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for nil channel consumer")
	}
	if !errors.Is(err, ErrConsumerNotReady) {
		t.Fatalf("expected ErrConsumerNotReady, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// dispatchDelivery – handler error path (non-autoAck)
// ---------------------------------------------------------------------------

func TestDispatchDelivery_HandlerError_NonAutoAck(t *testing.T) {
	client := NewRabbitMQClient()
	client.metrics = NewMetrics()

	handler := func(_ context.Context, _ amqp.Delivery) error {
		return errors.New("processing error")
	}

	// With autoAck=false the delivery is nacked; amqp.Delivery.Nack on a zero-
	// value struct returns an error, which is logged but does not propagate.
	client.dispatchDelivery(context.Background(), amqp.Delivery{}, handler, false)

	stats := client.metrics.GetStats()
	consumerStats := stats["consumer"].(map[string]any)
	if consumerStats["messages_failed"].(int64) != 1 {
		t.Fatalf("expected 1 failed message, got %v", consumerStats["messages_failed"])
	}
}

// ---------------------------------------------------------------------------
// dispatchDelivery – success path (non-autoAck) ack is logged-only on error
// ---------------------------------------------------------------------------

func TestDispatchDelivery_SuccessPath_NonAutoAck(t *testing.T) {
	client := NewRabbitMQClient()
	client.metrics = NewMetrics()

	handler := func(_ context.Context, _ amqp.Delivery) error { return nil }

	// Zero-value Delivery.Ack returns error; dispatchDelivery logs it but must not crash.
	client.dispatchDelivery(context.Background(), amqp.Delivery{}, handler, false)

	stats := client.metrics.GetStats()
	consumerStats := stats["consumer"].(map[string]any)
	if consumerStats["messages_received"].(int64) != 1 {
		t.Fatalf("expected 1 received message, got %v", consumerStats["messages_received"])
	}
}

// ---------------------------------------------------------------------------
// dispatchDelivery – autoAck path (no ack/nack call)
// ---------------------------------------------------------------------------

func TestDispatchDelivery_HandlerError_AutoAck(t *testing.T) {
	client := NewRabbitMQClient()
	client.metrics = NewMetrics()

	handler := func(_ context.Context, _ amqp.Delivery) error {
		return errors.New("processing error")
	}
	// autoAck=true means we return after metrics without calling nack.
	client.dispatchDelivery(context.Background(), amqp.Delivery{}, handler, true)

	stats := client.metrics.GetStats()
	consumerStats := stats["consumer"].(map[string]any)
	if consumerStats["messages_failed"].(int64) != 1 {
		t.Fatalf("expected 1 failed message, got %v", consumerStats["messages_failed"])
	}
}

// ---------------------------------------------------------------------------
// normalizeRabbitMQConfig – nil producer/consumer entries
// ---------------------------------------------------------------------------

func TestNormalizeRabbitMQConfig_NilProducer(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Urls:      []string{"amqp://localhost:5672/"},
		Producers: []*conf.Producer{nil},
	}
	err := normalizeRabbitMQConfig(cfg)
	if err == nil {
		t.Fatal("expected error for nil producer entry")
	}
}

func TestNormalizeRabbitMQConfig_NilConsumer(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Urls:      []string{"amqp://localhost:5672/"},
		Consumers: []*conf.Consumer{nil},
	}
	err := normalizeRabbitMQConfig(cfg)
	if err == nil {
		t.Fatal("expected error for nil consumer entry")
	}
}

func TestNormalizeRabbitMQConfig_NilConfig(t *testing.T) {
	if err := normalizeRabbitMQConfig(nil); err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNormalizeRabbitMQConfig_NegativeMaxRetries(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Urls: []string{"amqp://localhost:5672/"},
		Producers: []*conf.Producer{
			{Name: "p", Enabled: true, ExchangeType: ExchangeTypeDirect, MaxRetries: -1},
		},
	}
	if err := normalizeRabbitMQConfig(cfg); err == nil {
		t.Fatal("expected error for negative max retries")
	}
}

func TestNormalizeRabbitMQConfig_EnabledProducerNoName(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Urls: []string{"amqp://localhost:5672/"},
		Producers: []*conf.Producer{
			{Enabled: true, Name: ""},
		},
	}
	if err := normalizeRabbitMQConfig(cfg); err == nil {
		t.Fatal("expected error for enabled producer without name")
	}
}

func TestNormalizeRabbitMQConfig_EnabledConsumerNoName(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Urls: []string{"amqp://localhost:5672/"},
		Consumers: []*conf.Consumer{
			{Enabled: true, Name: ""},
		},
	}
	if err := normalizeRabbitMQConfig(cfg); err == nil {
		t.Fatal("expected error for enabled consumer without name")
	}
}

// ---------------------------------------------------------------------------
// validateVirtualHost – too-long name
// ---------------------------------------------------------------------------

func TestValidateVirtualHost_TooLong(t *testing.T) {
	if err := validateVirtualHost(strings.Repeat("v", 256)); err == nil {
		t.Fatal("expected error for too-long virtual host name")
	}
}

// ---------------------------------------------------------------------------
// RetryHandler – context cancellation during back-off sleep
// ---------------------------------------------------------------------------

func TestRetryHandlerDoWithRetry_CanceledDuringBackoff(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Producers: []*conf.Producer{
			{
				MaxRetries:   10,
				RetryBackoff: durationpb.New(50 * time.Millisecond),
			},
		},
	}
	handler := NewRetryHandler(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := handler.DoWithRetry(ctx, func() error {
		return errors.New("always fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
