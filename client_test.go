package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-rabbitmq/conf"
	"github.com/go-lynx/lynx/plugins"
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
