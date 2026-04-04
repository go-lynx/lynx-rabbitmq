package rabbitmq

import (
	"strings"
	"testing"
	"time"

	"github.com/go-lynx/lynx-rabbitmq/conf"
	"github.com/go-lynx/lynx/plugins"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRabbitMQRuntimeContract_StartupFailurePublishesUnreadyState(t *testing.T) {
	base := plugins.NewSimpleRuntime()
	rt := base.WithPluginContext(pluginName)

	client := NewRabbitMQClient()
	client.rt = rt
	client.config = &conf.RabbitMQ{
		Urls: []string{"amqp://guest:guest@127.0.0.1:1/"},
		Producers: []*conf.Producer{
			{Name: "p1", Enabled: true, Exchange: "orders", ExchangeType: ExchangeTypeDirect, PublishTimeout: durationpb.New(time.Millisecond)},
		},
		Consumers: []*conf.Consumer{
			{Name: "c1", Enabled: true, Queue: "orders", ConsumerTag: "orders", MaxConcurrency: 1, PrefetchCount: 1},
		},
		VirtualHost:     "/",
		DialTimeout:     durationpb.New(100 * time.Millisecond),
		Heartbeat:       durationpb.New(time.Second),
		ChannelPoolSize: 1,
	}
	client.healthChecker = NewHealthChecker()
	client.connectionManager = NewConnectionManager(client.config)
	client.retryHandler = NewRetryHandler(client.config)
	client.goroutinePool = NewGoroutinePool(1)

	err := client.StartupTasks()
	if err == nil {
		t.Fatal("expected StartupTasks to fail without a local RabbitMQ broker")
	}
	if !strings.Contains(err.Error(), "failed to connect") {
		t.Fatalf("unexpected startup error: %v", err)
	}

	if readiness, err := base.GetSharedResource(sharedReadinessResourceName); err != nil || readiness != false {
		t.Fatalf("unexpected shared readiness after failure: value=%#v err=%v", readiness, err)
	}
	if health, err := base.GetSharedResource(sharedHealthResourceName); err != nil || health != false {
		t.Fatalf("unexpected shared health after failure: value=%#v err=%v", health, err)
	}
	if readiness, err := rt.GetPrivateResource(privateReadinessResourceName); err != nil || readiness != false {
		t.Fatalf("unexpected private readiness after failure: value=%#v err=%v", readiness, err)
	}
	if health, err := rt.GetPrivateResource(privateHealthResourceName); err != nil || health != false {
		t.Fatalf("unexpected private health after failure: value=%#v err=%v", health, err)
	}
}
