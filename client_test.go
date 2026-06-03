package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-rabbitmq/conf"
	"github.com/go-lynx/lynx/plugins"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

type staticConfigSource struct {
	content string
}

func (s staticConfigSource) Load() ([]*config.KeyValue, error) {
	return []*config.KeyValue{{
		Key:    "runtime.yaml",
		Value:  []byte(s.content),
		Format: "yaml",
	}}, nil
}

func (s staticConfigSource) Watch() (config.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &staticConfigWatcher{ctx: ctx, cancel: cancel}, nil
}

type staticConfigWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *staticConfigWatcher) Next() ([]*config.KeyValue, error) {
	<-w.ctx.Done()
	return nil, w.ctx.Err()
}

func (w *staticConfigWatcher) Stop() error {
	w.cancel()
	return nil
}

func newRuntimeWithConfigFile(t *testing.T, content string) plugins.Runtime {
	t.Helper()

	cfg := config.New(config.WithSource(staticConfigSource{content: content}))
	if err := cfg.Load(); err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() {
		_ = cfg.Close()
	})

	rt := plugins.NewSimpleRuntime()
	rt.SetConfig(cfg)
	return rt
}

func TestNewRabbitMQClientDefaults(t *testing.T) {
	client := NewRabbitMQClient()

	if client.config == nil {
		t.Fatal("expected default config to be initialized")
	}
	if got := client.config.Urls; len(got) != 1 || got[0] != "amqp://localhost:5672/" {
		t.Fatalf("unexpected default URLs: %#v", got)
	}
	if client.config.VirtualHost != "/" {
		t.Fatalf("unexpected virtual host: %q", client.config.VirtualHost)
	}
	if got := len(client.GetEnabledProducers()); got != 1 {
		t.Fatalf("expected 1 enabled producer, got %d", got)
	}
	if got := len(client.GetEnabledConsumers()); got != 1 {
		t.Fatalf("expected 1 enabled consumer, got %d", got)
	}
	if client.IsConnected() {
		t.Fatal("expected new RabbitMQ client to start disconnected")
	}
}

func TestRetryHandlerDoWithRetry(t *testing.T) {
	cfg := &conf.RabbitMQ{
		Producers: []*conf.Producer{
			{
				MaxRetries:   2,
				RetryBackoff: durationpb.New(0),
			},
		},
	}
	handler := NewRetryHandler(cfg)

	t.Run("success after retries", func(t *testing.T) {
		attempts := 0
		err := handler.DoWithRetry(context.Background(), func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary failure")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("expected retry flow to succeed, got %v", err)
		}
		if attempts != 3 {
			t.Fatalf("expected 3 attempts, got %d", attempts)
		}
	})

	t.Run("context cancellation wins", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		attempts := 0
		err := handler.DoWithRetry(ctx, func() error {
			attempts++
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
		if attempts != 0 {
			t.Fatalf("expected operation not to run after cancellation, got %d attempts", attempts)
		}
	})

	t.Run("max retries exceeded", func(t *testing.T) {
		attempts := 0
		err := handler.DoWithRetry(context.Background(), func() error {
			attempts++
			return errors.New("permanent failure")
		})
		if err == nil {
			t.Fatal("expected retry flow to return an error")
		}
		if attempts != 3 {
			t.Fatalf("expected 3 attempts before failure, got %d", attempts)
		}
		if !strings.Contains(err.Error(), "max retries exceeded") {
			t.Fatalf("expected wrapped max retries message, got %v", err)
		}
	})
}

func TestRabbitMQMetricsAndManagers(t *testing.T) {
	metrics := NewMetrics()
	metrics.IncrementProducerMessagesSent()
	metrics.IncrementProducerMessagesFailed()
	metrics.RecordProducerLatency(2 * time.Second)
	metrics.IncrementConsumerMessagesReceived()
	metrics.RecordConsumerLatency(500 * time.Millisecond)
	metrics.IncrementConnectionErrors()
	metrics.IncrementReconnectionCount()
	metrics.IncrementHealthCheckCount()
	metrics.SetHealthy(true)
	metrics.UpdateLastHealthCheck()

	stats := metrics.GetStats()
	producerStats := stats["producer"].(map[string]interface{})
	if producerStats["messages_sent"] != int64(1) {
		t.Fatalf("unexpected producer sent count: %#v", producerStats["messages_sent"])
	}
	if producerStats["messages_failed"] != int64(1) {
		t.Fatalf("unexpected producer failed count: %#v", producerStats["messages_failed"])
	}

	healthStats := stats["health"].(map[string]interface{})
	if healthStats["is_healthy"] != true {
		t.Fatalf("expected health stats to report healthy, got %#v", healthStats["is_healthy"])
	}

	prometheusText := metrics.GetPrometheusMetrics()
	if !strings.Contains(prometheusText, "lynx_rabbitmq_producer_messages_sent_total 1") {
		t.Fatalf("expected producer counter in Prometheus output, got:\n%s", prometheusText)
	}
	if !strings.Contains(prometheusText, "lynx_rabbitmq_health_status 1") {
		t.Fatalf("expected health gauge in Prometheus output, got:\n%s", prometheusText)
	}

	metrics.Reset()
	resetStats := metrics.GetStats()
	resetProducerStats := resetStats["producer"].(map[string]interface{})
	if resetProducerStats["messages_sent"] != int64(0) {
		t.Fatalf("expected reset producer count to be 0, got %#v", resetProducerStats["messages_sent"])
	}

	connectionManager := NewConnectionManager(&conf.RabbitMQ{})
	connectionManager.Start()
	if !connectionManager.IsConnected() {
		t.Fatal("expected connection manager to report connected after Start")
	}
	connectionManager.ForceReconnect()
	if !connectionManager.IsConnected() {
		t.Fatal("expected connection manager to remain connected after ForceReconnect")
	}
	connectionManager.Stop()
	connectionManager.Stop()
	if connectionManager.IsConnected() {
		t.Fatal("expected connection manager to be disconnected after Stop")
	}

	healthChecker := NewHealthChecker()
	healthChecker.performHealthCheck()
	if healthChecker.GetLastCheck().IsZero() {
		t.Fatal("expected performHealthCheck to update last check time")
	}
	healthChecker.Stop()
	healthChecker.Stop()

	client := NewRabbitMQClient()
	client.config.Producers = []*conf.Producer{
		{Name: "enabled", Enabled: true},
		{Name: "disabled", Enabled: false},
	}
	client.config.Consumers = []*conf.Consumer{
		{Name: "enabled", Enabled: true},
		{Name: "disabled", Enabled: false},
	}
	if got := len(client.GetEnabledProducers()); got != 1 {
		t.Fatalf("expected 1 enabled producer after filtering, got %d", got)
	}
	if got := len(client.GetEnabledConsumers()); got != 1 {
		t.Fatalf("expected 1 enabled consumer after filtering, got %d", got)
	}
}

func TestInitializeResourcesLoadsRuntimeConfig(t *testing.T) {
	rt := newRuntimeWithConfigFile(t, `
rabbitmq:
  urls:
    - "amqp://service:secret@mq:5672/"
  producers:
    - name: "orders-producer"
      enabled: true
      exchange: "orders.exchange"
  consumers:
    - name: "orders-consumer"
      enabled: true
      queue: "orders.queue"
  virtual_host: "/orders"
`)

	client := NewRabbitMQClient()
	if err := client.InitializeResources(rt); err != nil {
		t.Fatalf("InitializeResources returned error: %v", err)
	}

	if got := client.config.Urls[0]; got != "amqp://service:secret@mq:5672/" {
		t.Fatalf("unexpected url after scan: %q", got)
	}
	if client.config.VirtualHost != "/orders" {
		t.Fatalf("unexpected virtual host: %q", client.config.VirtualHost)
	}
	if client.config.DialTimeout == nil || client.config.DialTimeout.AsDuration() != 3*time.Second {
		t.Fatalf("expected default dial timeout, got %#v", client.config.DialTimeout)
	}
	if client.config.Heartbeat == nil || client.config.Heartbeat.AsDuration() != 30*time.Second {
		t.Fatalf("expected default heartbeat, got %#v", client.config.Heartbeat)
	}
	if client.config.Producers[0].ExchangeType != ExchangeTypeDirect {
		t.Fatalf("expected default exchange type, got %q", client.config.Producers[0].ExchangeType)
	}
	if client.config.Consumers[0].ConsumerTag != defaultConsumer {
		t.Fatalf("expected default consumer tag, got %q", client.config.Consumers[0].ConsumerTag)
	}
	if client.healthChecker == nil || client.connectionManager == nil || client.retryHandler == nil || client.goroutinePool == nil {
		t.Fatal("expected InitializeResources to build managers")
	}
}

func TestInitializeResourcesRejectsInvalidConfig(t *testing.T) {
	rt := newRuntimeWithConfigFile(t, `
rabbitmq:
  urls:
    - ""
`)

	client := NewRabbitMQClient()
	if err := client.InitializeResources(rt); err == nil {
		t.Fatal("expected InitializeResources to reject empty URL entries")
	}
}

func TestInitializeResourcesRejectsInvalidExchangeType(t *testing.T) {
	rt := newRuntimeWithConfigFile(t, `
rabbitmq:
  producers:
    - name: "orders-producer"
      enabled: true
      exchange_type: "invalid"
`)

	client := NewRabbitMQClient()
	if err := client.InitializeResources(rt); err == nil {
		t.Fatal("expected InitializeResources to reject invalid exchange_type")
	}
}

// --------------------------------------------------------------------------
// Validation unit tests (validateExchange / validateQueue / validateVirtualHost)
// --------------------------------------------------------------------------

func TestValidateExchange(t *testing.T) {
	if err := validateExchange("orders.exchange"); err != nil {
		t.Fatalf("expected valid exchange to pass, got %v", err)
	}
	if err := validateExchange(""); err == nil {
		t.Fatal("expected empty exchange to fail")
	}
	if err := validateExchange(strings.Repeat("a", 256)); err == nil {
		t.Fatal("expected too-long exchange name to fail")
	}
	if err := validateExchange("bad exchange"); err == nil {
		t.Fatal("expected exchange with space to fail")
	}
}

func TestValidateQueue(t *testing.T) {
	if err := validateQueue("orders.queue"); err != nil {
		t.Fatalf("expected valid queue to pass, got %v", err)
	}
	if err := validateQueue(""); err == nil {
		t.Fatal("expected empty queue to fail")
	}
	if err := validateQueue(strings.Repeat("q", 256)); err == nil {
		t.Fatal("expected too-long queue name to fail")
	}
	if err := validateQueue("bad queue"); err == nil {
		t.Fatal("expected queue with space to fail")
	}
}

func TestValidateVirtualHost(t *testing.T) {
	if err := validateVirtualHost("/"); err != nil {
		t.Fatalf("expected root vhost to pass, got %v", err)
	}
	if err := validateVirtualHost(""); err == nil {
		t.Fatal("expected empty vhost to fail")
	}
}

func TestValidateExchangeType(t *testing.T) {
	for _, valid := range []string{ExchangeTypeDirect, ExchangeTypeFanout, ExchangeTypeTopic, ExchangeTypeHeaders} {
		if err := validateExchangeType(valid); err != nil {
			t.Fatalf("expected %q to be valid, got %v", valid, err)
		}
	}
	if err := validateExchangeType("unknown"); err == nil {
		t.Fatal("expected unknown exchange type to fail")
	}
}

// --------------------------------------------------------------------------
// PublishMessage / Subscribe guard-rail tests (no live broker required)
// --------------------------------------------------------------------------

func TestPublishMessage_NoProducer(t *testing.T) {
	client := NewRabbitMQClient()
	// No producers registered.
	err := client.PublishMessage(context.Background(), "ex", "key", []byte("hello"))
	if err == nil {
		t.Fatal("expected error when no producer is registered")
	}
	if !errors.Is(err, ErrProducerNotReady) {
		t.Fatalf("expected ErrProducerNotReady, got %v", err)
	}
}

func TestPublishMessage_ValidationErrors(t *testing.T) {
	client := NewRabbitMQClient()
	// Register a fake non-nil channel entry so we pass the nil check.
	client.producers["fake"] = &amqp.Channel{}

	// Empty body should be rejected.
	if err := client.PublishMessage(context.Background(), "ex", "key", nil); err == nil {
		t.Fatal("expected error for empty body")
	}
	// Invalid exchange name should be rejected before any network call.
	if err := client.PublishMessage(context.Background(), "bad exchange", "key", []byte("x")); err == nil {
		t.Fatal("expected error for exchange name with space")
	}
}

func TestPublishMessageWith_NotFound(t *testing.T) {
	client := NewRabbitMQClient()
	err := client.PublishMessageWith(context.Background(), "missing", "ex", "key", []byte("hello"))
	if !errors.Is(err, ErrProducerNotFound) {
		t.Fatalf("expected ErrProducerNotFound, got %v", err)
	}
}

func TestGetProducer_GetConsumer_NotFound(t *testing.T) {
	client := NewRabbitMQClient()
	if _, err := client.GetProducer("none"); !errors.Is(err, ErrProducerNotFound) {
		t.Fatalf("expected ErrProducerNotFound, got %v", err)
	}
	if _, err := client.GetConsumer("none"); !errors.Is(err, ErrConsumerNotFound) {
		t.Fatalf("expected ErrConsumerNotFound, got %v", err)
	}
}

func TestIsProducerReady_IsConsumerReady(t *testing.T) {
	client := NewRabbitMQClient()
	if client.IsProducerReady("none") {
		t.Fatal("expected false for missing producer")
	}
	client.producers["p"] = nil
	if client.IsProducerReady("p") {
		t.Fatal("expected false for nil producer channel")
	}
	client.producers["p"] = &amqp.Channel{}
	if !client.IsProducerReady("p") {
		t.Fatal("expected true for non-nil producer channel")
	}
	if client.IsConsumerReady("none") {
		t.Fatal("expected false for missing consumer")
	}
}

func TestSubscribeWith_ValidationError(t *testing.T) {
	client := NewRabbitMQClient()
	// Register a fake channel so the lookup succeeds.
	client.consumers["c"] = &amqp.Channel{}
	// Invalid queue name (contains space) should be caught by validation.
	err := client.SubscribeWith(context.Background(), "c", "bad queue", func(_ context.Context, _ amqp.Delivery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for queue name with space")
	}
}

func TestSubscribeWith_NotFound(t *testing.T) {
	client := NewRabbitMQClient()
	err := client.SubscribeWith(context.Background(), "missing", "queue", func(_ context.Context, _ amqp.Delivery) error {
		return nil
	})
	if !errors.Is(err, ErrConsumerNotFound) {
		t.Fatalf("expected ErrConsumerNotFound, got %v", err)
	}
}

// --------------------------------------------------------------------------
// dispatchDelivery panic recovery test
// --------------------------------------------------------------------------

func TestDispatchDelivery_PanicRecovery(t *testing.T) {
	client := NewRabbitMQClient()
	client.metrics = NewMetrics()

	panicHandler := func(_ context.Context, _ amqp.Delivery) error {
		panic("deliberate test panic")
	}

	// Should not propagate the panic.
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.dispatchDelivery(context.Background(), amqp.Delivery{}, panicHandler, true)
	}()
	select {
	case <-done:
		// good — panic was recovered
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchDelivery did not return after panic")
	}

	stats := client.metrics.GetStats()
	consumerStats := stats["consumer"].(map[string]any)
	if consumerStats["messages_failed"].(int64) != 1 {
		t.Fatalf("expected 1 failed message after panic, got %v", consumerStats["messages_failed"])
	}
}

// --------------------------------------------------------------------------
// buildAMQPURL helper test
// --------------------------------------------------------------------------

func TestBuildAMQPURL(t *testing.T) {
	url := buildAMQPURL("user", "pass", "host", "5672", "/vhost")
	if url != "amqp://user:pass@host:5672//vhost" {
		t.Fatalf("unexpected URL: %s", url)
	}
	// defaults
	def := buildAMQPURL("", "", "", "", "")
	if def != "amqp://guest:guest@localhost:5672//" {
		t.Fatalf("unexpected default URL: %s", def)
	}
}

// --------------------------------------------------------------------------
// ConnectionManager reconnect callback test
// --------------------------------------------------------------------------

func TestConnectionManager_SetReconnectFunc(t *testing.T) {
	cfg := &conf.RabbitMQ{}
	cm := NewConnectionManager(cfg)

	called := false
	cm.SetReconnectFunc(func() (*amqp.Connection, error) {
		called = true
		return nil, errors.New("no broker")
	})

	cm.Start()
	cm.ForceReconnect()
	if !called {
		t.Fatal("expected reconnect callback to be invoked by ForceReconnect")
	}
	cm.Stop()
}

// --------------------------------------------------------------------------
// GoroutinePool tests
// --------------------------------------------------------------------------

func TestGoroutinePool_SubmitAndWait(t *testing.T) {
	pool := NewGoroutinePool(4)

	var count int
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		pool.Submit(func() {
			mu.Lock()
			count++
			mu.Unlock()
		})
	}
	pool.Wait()
	mu.Lock()
	defer mu.Unlock()
	if count != 10 {
		t.Fatalf("expected 10 tasks to run, got %d", count)
	}
}

func TestGoroutinePool_SubmitAfterClose(t *testing.T) {
	pool := NewGoroutinePool(2)
	pool.Wait() // closes pool

	// Submitting after close must not panic.
	pool.Submit(func() {})
}

// --------------------------------------------------------------------------
// buildQueueArgs DLQ test
// --------------------------------------------------------------------------

func TestBuildQueueArgs_WithDLX(t *testing.T) {
	cfg := &conf.Consumer{
		Exchange:   "dlx.exchange",
		RoutingKey: "dlq.key",
	}
	args := buildQueueArgs(cfg)
	if args == nil {
		t.Fatal("expected non-nil args for DLX consumer")
	}
	if args["x-dead-letter-exchange"] != "dlx.exchange" {
		t.Fatalf("unexpected DLX: %v", args["x-dead-letter-exchange"])
	}
	if args["x-dead-letter-routing-key"] != "dlq.key" {
		t.Fatalf("unexpected DLQ routing key: %v", args["x-dead-letter-routing-key"])
	}
}

func TestBuildQueueArgs_WithoutDLX(t *testing.T) {
	cfg := &conf.Consumer{Queue: "q"}
	if args := buildQueueArgs(cfg); args != nil {
		t.Fatalf("expected nil args when no DLX configured, got %v", args)
	}
}

// --------------------------------------------------------------------------
// parseDuration helper test
// --------------------------------------------------------------------------

func TestParseDuration(t *testing.T) {
	if got := parseDuration("5s", time.Second); got != 5*time.Second {
		t.Fatalf("expected 5s, got %v", got)
	}
	if got := parseDuration("bad", time.Second); got != time.Second {
		t.Fatalf("expected default on bad input, got %v", got)
	}
	if got := parseDuration("", 2*time.Second); got != 2*time.Second {
		t.Fatalf("expected default on empty input, got %v", got)
	}
}
