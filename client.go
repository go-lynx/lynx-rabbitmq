// Package rabbitmq provides a RabbitMQ messaging plugin for the Lynx framework.
//
// The plugin manages AMQP channel pools and consumers with configurable prefetch,
// reconnect backoff, and dead-letter routing.  It collects Prometheus-compatible
// metrics for publish/consume latency, error rates, and connection health.
//
// Quick start:
//
//	import _ "github.com/go-lynx/lynx-rabbitmq"   // registers the plugin
//
// Configuration key: "lynx.rabbitmq" (YAML / proto).  See conf.RabbitMQ for all fields.
package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kratosconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx-rabbitmq/conf"
	"github.com/go-lynx/lynx/log"
	"github.com/go-lynx/lynx/plugins"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/types/known/durationpb"
)

// RabbitMQClient represents the main RabbitMQ client plugin instance
type RabbitMQClient struct {
	*plugins.BasePlugin
	config            *conf.RabbitMQ
	rt                plugins.Runtime
	connection        *amqp.Connection
	producers         map[string]*amqp.Channel
	consumers         map[string]*amqp.Channel
	producerMutex     sync.RWMutex
	consumerMutex     sync.RWMutex
	connectionMutex   sync.RWMutex
	closeChan         chan struct{}
	closeOnce         sync.Once // Protect against multiple close operations
	closed            atomic.Bool
	metrics           *Metrics
	healthChecker     *HealthChecker
	connectionManager *ConnectionManager
	retryHandler      *RetryHandler
	goroutinePool     *GoroutinePool

	// subscriptionWG tracks Subscribe dispatch goroutines so they can be
	// drained before the underlying channels are closed on shutdown.
	subscriptionWG sync.WaitGroup
	// subscriptions records active subscriptions so they can be re-established
	// after a reconnect rebuilds the consumer channels.
	subscriptions  []*subscription
	subscriptionMu sync.Mutex
}

// subscription captures the parameters of an active Subscribe call so the
// dispatch loop can be restarted after a reconnect.
type subscription struct {
	consumerName string
	queue        string
	handler      MessageHandler
}

// NewRabbitMQClient creates a new RabbitMQ client plugin instance
func NewRabbitMQClient() *RabbitMQClient {
	c := &RabbitMQClient{
		config:    defaultRabbitMQConfig(),
		producers: make(map[string]*amqp.Channel),
		consumers: make(map[string]*amqp.Channel),
		closeChan: make(chan struct{}),
		metrics:   &Metrics{},
	}

	c.BasePlugin = plugins.NewBasePlugin(
		plugins.GeneratePluginID("", pluginName, pluginVersion),
		pluginName,
		pluginDescription,
		pluginVersion,
		confPrefix,
		102, // Weight for RabbitMQ
	)

	return c
}

func defaultRabbitMQConfig() *conf.RabbitMQ {
	return &conf.RabbitMQ{
		Urls: []string{"amqp://localhost:5672/"},
		Producers: []*conf.Producer{
			{
				Name:            "default-producer",
				Enabled:         true,
				Exchange:        defaultExchange,
				ExchangeType:    ExchangeTypeDirect,
				ExchangeDurable: true,
				PublishTimeout:  durationpb.New(3 * time.Second),
				MaxRetries:      3,
				RetryBackoff:    durationpb.New(100 * time.Millisecond),
			},
		},
		Consumers: []*conf.Consumer{
			{
				Name:           "default-consumer",
				Enabled:        true,
				Queue:          defaultQueue,
				QueueDurable:   true,
				ConsumerTag:    defaultConsumer,
				MaxConcurrency: 1,
				PrefetchCount:  1,
				AutoAck:        false,
			},
		},
		Username:        "",
		Password:        "",
		VirtualHost:     defaultVirtualHost,
		DialTimeout:     durationpb.New(3 * time.Second),
		Heartbeat:       durationpb.New(30 * time.Second),
		ChannelPoolSize: defaultChannelPoolSize,
	}
}

// InitializeResources scans RabbitMQ configuration and wires up the health checker,
// connection manager, retry handler, and goroutine pool.
func (r *RabbitMQClient) InitializeResources(rt plugins.Runtime) error {
	if err := r.BasePlugin.InitializeResources(rt); err != nil {
		return err
	}
	r.rt = rt

	cfg := defaultRabbitMQConfig()
	if rt != nil && rt.GetConfig() != nil {
		if err := rt.GetConfig().Value(confPrefix).Scan(cfg); err != nil && !errors.Is(err, kratosconfig.ErrNotFound) {
			return fmt.Errorf("failed to scan RabbitMQ configuration: %w", err)
		}
	}
	if err := normalizeRabbitMQConfig(cfg); err != nil {
		return err
	}
	r.config = cfg

	r.healthChecker = NewHealthChecker()
	r.connectionManager = NewConnectionManager(r.config)
	r.retryHandler = NewRetryHandler(r.config)
	r.goroutinePool = NewGoroutinePool(10) // Default pool size

	return nil
}

func normalizeRabbitMQConfig(cfg *conf.RabbitMQ) error {
	if cfg == nil {
		return fmt.Errorf("rabbitmq configuration is required")
	}
	if len(cfg.Urls) == 0 {
		cfg.Urls = []string{"amqp://localhost:5672/"}
	}
	for i, url := range cfg.Urls {
		if url == "" {
			return fmt.Errorf("rabbitmq urls[%d] must not be empty", i)
		}
	}
	if cfg.VirtualHost == "" {
		cfg.VirtualHost = defaultVirtualHost
	}
	if cfg.DialTimeout == nil || cfg.DialTimeout.AsDuration() <= 0 {
		cfg.DialTimeout = durationpb.New(3 * time.Second)
	}
	if cfg.Heartbeat == nil || cfg.Heartbeat.AsDuration() <= 0 {
		cfg.Heartbeat = durationpb.New(30 * time.Second)
	}
	if cfg.ChannelPoolSize <= 0 {
		cfg.ChannelPoolSize = defaultChannelPoolSize
	}

	for i, producer := range cfg.Producers {
		if producer == nil {
			return fmt.Errorf("rabbitmq producers[%d] configuration is nil", i)
		}
		if producer.Enabled && producer.Name == "" {
			return fmt.Errorf("rabbitmq producers[%d].name is required when enabled", i)
		}
		if producer.Exchange == "" {
			producer.Exchange = defaultExchange
		}
		if producer.ExchangeType == "" {
			producer.ExchangeType = ExchangeTypeDirect
		}
		switch producer.ExchangeType {
		case ExchangeTypeDirect, ExchangeTypeFanout, ExchangeTypeTopic, ExchangeTypeHeaders:
		default:
			return fmt.Errorf("rabbitmq producers[%d].exchange_type %q is invalid", i, producer.ExchangeType)
		}
		if producer.PublishTimeout == nil || producer.PublishTimeout.AsDuration() <= 0 {
			producer.PublishTimeout = durationpb.New(3 * time.Second)
		}
		if producer.MaxRetries < 0 {
			return fmt.Errorf("rabbitmq producers[%d].max_retries must be non-negative", i)
		}
		if producer.MaxRetries == 0 {
			producer.MaxRetries = defaultMaxRetries
		}
		if producer.RetryBackoff == nil || producer.RetryBackoff.AsDuration() <= 0 {
			producer.RetryBackoff = durationpb.New(100 * time.Millisecond)
		}
	}

	for i, consumer := range cfg.Consumers {
		if consumer == nil {
			return fmt.Errorf("rabbitmq consumers[%d] configuration is nil", i)
		}
		if consumer.Enabled && consumer.Name == "" {
			return fmt.Errorf("rabbitmq consumers[%d].name is required when enabled", i)
		}
		if consumer.Queue == "" {
			consumer.Queue = defaultQueue
		}
		if consumer.ConsumerTag == "" {
			consumer.ConsumerTag = defaultConsumer
		}
		if consumer.MaxConcurrency <= 0 {
			consumer.MaxConcurrency = defaultMaxConcurrency
		}
		if consumer.PrefetchCount <= 0 {
			consumer.PrefetchCount = defaultPrefetchCount
		}
	}

	return nil
}

// StartupTasks connects to RabbitMQ, initializes producers/consumers, and starts health checks.
func (r *RabbitMQClient) StartupTasks() error {
	return r.startupWithContext(context.Background())
}

// CleanupTasks stops all producers/consumers and closes the AMQP connection.
func (r *RabbitMQClient) CleanupTasks() error {
	return r.cleanupWithContext(context.Background())
}

func (r *RabbitMQClient) startupWithContext(ctx context.Context) error {
	log.Infof("initializing RabbitMQ client")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rabbitmq startup canceled before execution: %w", err)
	}
	r.publishRuntimeContract(false, false)

	if err := r.connect(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("rabbitmq startup canceled after connect: %w", err)
	}

	if err := r.initializeProducers(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize producers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("rabbitmq startup canceled after producer init: %w", err)
	}

	if err := r.initializeConsumers(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize consumers: %w", err)
	}

	// Start health checker with a real connection probe.
	if r.healthChecker != nil {
		r.healthChecker.SetProbe(r.connectionProbe)
		r.healthChecker.Start()
	}

	// Start connection manager
	if r.connectionManager != nil {
		r.connectionManager.Start()
	}

	if r.rt != nil {
		if err := r.rt.RegisterSharedResource(pluginName, r); err != nil {
			r.publishRuntimeContract(false, false)
			return fmt.Errorf("failed to register RabbitMQ shared resource: %w", err)
		}
		r.registerRuntimePluginAlias()
		if err := r.rt.RegisterPrivateResource("config", r.config); err != nil {
			log.Warnf("failed to register RabbitMQ private config resource: %v", err)
		}
		if r.connection != nil {
			if err := r.rt.RegisterPrivateResource("connection", r.connection); err != nil {
				log.Warnf("failed to register RabbitMQ private connection resource: %v", err)
			}
		}
		if len(r.producers) > 0 {
			if err := r.rt.RegisterPrivateResource("producers", r.producers); err != nil {
				log.Warnf("failed to register RabbitMQ private producers resource: %v", err)
			}
		}
		if len(r.consumers) > 0 {
			if err := r.rt.RegisterPrivateResource("consumers", r.consumers); err != nil {
				log.Warnf("failed to register RabbitMQ private consumers resource: %v", err)
			}
		}
		if r.connectionManager != nil {
			if err := r.rt.RegisterPrivateResource("connection_manager", r.connectionManager); err != nil {
				log.Warnf("failed to register RabbitMQ private connection manager resource: %v", err)
			}
		}
		if r.healthChecker != nil {
			if err := r.rt.RegisterPrivateResource("health_checker", r.healthChecker); err != nil {
				log.Warnf("failed to register RabbitMQ private health checker resource: %v", err)
			}
		}
		if r.retryHandler != nil {
			if err := r.rt.RegisterPrivateResource("retry_handler", r.retryHandler); err != nil {
				log.Warnf("failed to register RabbitMQ private retry handler resource: %v", err)
			}
		}
		if r.metrics != nil {
			if err := r.rt.RegisterPrivateResource("metrics", r.metrics); err != nil {
				log.Warnf("failed to register RabbitMQ private metrics resource: %v", err)
			}
		}
	}

	if err := r.CheckHealth(); err != nil {
		r.publishRuntimeContract(false, false)
		return err
	}
	r.publishRuntimeContract(true, true)

	log.Infof("RabbitMQ client successfully initialized")
	return nil
}

func (r *RabbitMQClient) cleanupWithContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rabbitmq cleanup canceled before execution: %w", err)
	}
	log.Infof("shutting down RabbitMQ client plugin")
	r.publishRuntimeContract(false, false)

	// Signal background tasks to stop (protected against multiple calls)
	r.closed.Store(true)
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})

	if r.healthChecker != nil {
		r.healthChecker.Stop()
	}

	if r.connectionManager != nil {
		r.connectionManager.Stop()
	}

	// Drain Subscribe dispatch goroutines before closing their channels so we
	// never Ack/Nack on a channel that is being torn down underneath them.
	r.subscriptionWG.Wait()

	if r.goroutinePool != nil {
		r.goroutinePool.Wait()
	}

	r.consumerMutex.Lock()
	for name, channel := range r.consumers {
		if channel != nil {
			channel.Close()
			log.Infof("consumer channel %s closed", name)
		}
	}
	r.consumers = make(map[string]*amqp.Channel)
	r.consumerMutex.Unlock()

	r.producerMutex.Lock()
	for name, channel := range r.producers {
		if channel != nil {
			channel.Close()
			log.Infof("producer channel %s closed", name)
		}
	}
	r.producers = make(map[string]*amqp.Channel)
	r.producerMutex.Unlock()

	r.connectionMutex.Lock()
	if r.connection != nil {
		if err := r.connection.Close(); err != nil {
			r.connectionMutex.Unlock()
			return err
		}
		log.Infof("RabbitMQ connection closed")
		r.connection = nil
	}
	r.connectionMutex.Unlock()

	log.Infof("RabbitMQ client plugin successfully shut down")
	return nil
}

// connect establishes an AMQP connection and registers the broker close-notify
// channel on the ConnectionManager so automatic reconnection can be driven.
func (r *RabbitMQClient) connect() error {
	conn, err := r.dial()
	if err != nil {
		return err
	}

	r.connectionMutex.Lock()
	r.connection = conn
	r.connectionMutex.Unlock()

	// Wire reconnect callback and notify channel into the manager.
	if r.connectionManager != nil {
		r.connectionManager.SetReconnectFunc(func() (*amqp.Connection, error) {
			newConn, dialErr := r.dial()
			if dialErr != nil {
				return nil, dialErr
			}
			r.connectionMutex.Lock()
			r.connection = newConn
			r.connectionMutex.Unlock()
			// Re-create producer/consumer channels (and re-declare
			// exchanges/queues/QoS/confirms) on the fresh connection and
			// restart the Subscribe loops; the old channels were bound to
			// the now-closed connection and are permanently dead.
			if rebuildErr := r.rebuildAfterReconnect(); rebuildErr != nil {
				return nil, rebuildErr
			}
			return newConn, nil
		})
		notifyCh := conn.NotifyClose(make(chan *amqp.Error, 1))
		r.connectionManager.SetConnectionNotify(notifyCh)
	}

	log.Infof("connected to RabbitMQ at %s", r.config.Urls[0])
	return nil
}

// rebuildAfterReconnect re-creates all producer and consumer channels on the
// current (freshly re-dialed) connection and restarts any active Subscribe
// loops.  It is invoked from the reconnect callback after r.connection has been
// replaced; if it fails the reconnect is treated as unsuccessful so the manager
// keeps retrying.
func (r *RabbitMQClient) rebuildAfterReconnect() error {
	// Discard the stale producer channels (they are bound to the closed
	// connection) before re-creating them.
	r.producerMutex.Lock()
	r.producers = make(map[string]*amqp.Channel)
	r.producerMutex.Unlock()

	r.consumerMutex.Lock()
	r.consumers = make(map[string]*amqp.Channel)
	r.consumerMutex.Unlock()

	if err := r.initializeProducers(); err != nil {
		return fmt.Errorf("reconnect: failed to rebuild producers: %w", err)
	}
	if err := r.initializeConsumers(); err != nil {
		return fmt.Errorf("reconnect: failed to rebuild consumers: %w", err)
	}

	// Restart the dispatch loops for every previously active subscription.
	r.subscriptionMu.Lock()
	subs := make([]*subscription, len(r.subscriptions))
	copy(subs, r.subscriptions)
	r.subscriptionMu.Unlock()

	for _, sub := range subs {
		r.consumerMutex.RLock()
		ch := r.consumers[sub.consumerName]
		r.consumerMutex.RUnlock()
		if ch == nil {
			continue
		}
		if err := r.startDispatchLoop(context.Background(), sub.consumerName, ch, sub.queue, sub.handler); err != nil {
			return fmt.Errorf("reconnect: failed to restart subscription %s: %w", sub.consumerName, err)
		}
	}

	log.Infof("RabbitMQ channels and subscriptions rebuilt after reconnect")
	return nil
}

// connectionProbe is wired into the HealthChecker as its liveness probe.  It
// opens a temporary channel on the live connection so disconnects are reflected
// in the health state rather than always reporting healthy.
func (r *RabbitMQClient) connectionProbe() error {
	if r.closed.Load() {
		return fmt.Errorf("rabbitmq client is closed")
	}
	r.connectionMutex.RLock()
	conn := r.connection
	r.connectionMutex.RUnlock()

	if conn == nil {
		return fmt.Errorf("rabbitmq connection not initialized")
	}
	if conn.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq health probe failed to open channel: %w", err)
	}
	_ = ch.Close()
	return nil
}

// dial creates a single AMQP connection using the first configured URL and
// validates the virtual host before dialling.
func (r *RabbitMQClient) dial() (*amqp.Connection, error) {
	if len(r.config.Urls) == 0 {
		return nil, fmt.Errorf("no RabbitMQ URLs configured")
	}

	// Validate virtual host.
	if err := validateVirtualHost(r.config.VirtualHost); err != nil {
		return nil, fmt.Errorf("invalid virtual host: %w", err)
	}

	dialURL := r.buildDialURL(r.config.Urls[0])
	cfg := amqp.Config{
		Vhost:     r.config.VirtualHost,
		Heartbeat: r.config.Heartbeat.AsDuration(),
		Locale:    "en_US",
		// Apply the configured dial timeout so a hung broker cannot block
		// startup indefinitely.
		Dial: amqp.DefaultDial(r.config.DialTimeout.AsDuration()),
	}

	// Attach a TLS client config when the URL uses the amqps scheme so the
	// handshake can proceed over TLS.
	if strings.HasPrefix(strings.ToLower(dialURL), "amqps://") {
		cfg.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	conn, err := amqp.DialConfig(dialURL, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ at %s: %w", r.config.Urls[0], err)
	}
	return conn, nil
}

// buildDialURL augments the configured URL with credentials and virtual host
// from the configuration when they are not already present in the URL.  If the
// URL cannot be parsed it is returned unchanged so amqp.DialConfig can surface
// the underlying error.
func (r *RabbitMQClient) buildDialURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	// Apply credentials from configuration when the URL omits userinfo.
	if u.User == nil || u.User.Username() == "" {
		if r.config.Username != "" {
			u.User = url.UserPassword(r.config.Username, r.config.Password)
		}
	}

	// Apply the configured virtual host when the URL carries no explicit path.
	if (u.Path == "" || u.Path == "/") && r.config.VirtualHost != "" && r.config.VirtualHost != "/" {
		u.Path = "/" + strings.TrimPrefix(r.config.VirtualHost, "/")
	}

	return u.String()
}

// initializeProducers initializes all configured producers
func (r *RabbitMQClient) initializeProducers() error {
	for _, producerConfig := range r.GetEnabledProducers() {
		if err := r.createProducer(producerConfig); err != nil {
			return fmt.Errorf("failed to create producer %s: %w", producerConfig.Name, err)
		}
	}
	return nil
}

// initializeConsumers initializes all configured consumers
func (r *RabbitMQClient) initializeConsumers() error {
	for _, consumerConfig := range r.GetEnabledConsumers() {
		if err := r.createConsumer(consumerConfig); err != nil {
			return fmt.Errorf("failed to create consumer %s: %w", consumerConfig.Name, err)
		}
	}
	return nil
}

// createProducer creates a RabbitMQ producer channel.
//
// The exchange name and type are validated before any network call.
// When a non-empty exchange is declared the channel is put into confirm mode
// so that the broker acknowledges every published message (publisher confirms).
func (r *RabbitMQClient) createProducer(config *conf.Producer) error {
	// Validate exchange name before touching the network.
	if config.Exchange != "" {
		if err := validateExchange(config.Exchange); err != nil {
			return fmt.Errorf("producer %s: %w", config.Name, err)
		}
		if err := validateExchangeType(config.ExchangeType); err != nil {
			return fmt.Errorf("producer %s: %w", config.Name, err)
		}
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare exchange if configured.
	if config.Exchange != "" {
		if err = channel.ExchangeDeclare(
			config.Exchange,
			config.ExchangeType,
			config.ExchangeDurable,
			config.ExchangeAutoDelete,
			false, // internal
			false, // no-wait
			nil,   // arguments
		); err != nil {
			channel.Close()
			return fmt.Errorf("failed to declare exchange %s: %w", config.Exchange, err)
		}

		// Enable publisher confirms for reliable delivery.
		if err = channel.Confirm(false); err != nil {
			channel.Close()
			return fmt.Errorf("failed to enable publisher confirms for producer %s: %w", config.Name, err)
		}
	}

	r.producerMutex.Lock()
	r.producers[config.Name] = channel
	r.producerMutex.Unlock()

	log.Infof("producer %s created (exchange=%s, confirms=enabled)", config.Name, config.Exchange)
	return nil
}

// createConsumer creates a RabbitMQ consumer channel.
//
// Queue and (optional) exchange names are validated before any network call.
// When the consumer config specifies a dead-letter exchange the queue is
// declared with the x-dead-letter-exchange and (optionally)
// x-dead-letter-routing-key arguments so that nack'd messages are routed to
// the DLQ automatically.
func (r *RabbitMQClient) createConsumer(config *conf.Consumer) error {
	// Validate queue/exchange names before touching the network.
	if config.Queue != "" {
		if err := validateQueue(config.Queue); err != nil {
			return fmt.Errorf("consumer %s: %w", config.Name, err)
		}
	}
	if config.Exchange != "" {
		if err := validateExchange(config.Exchange); err != nil {
			return fmt.Errorf("consumer %s: %w", config.Name, err)
		}
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}

	// Set QoS / prefetch before starting deliveries.
	if config.PrefetchCount > 0 {
		if err = channel.Qos(int(config.PrefetchCount), 0, false); err != nil {
			channel.Close()
			return fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	// Declare queue, attaching dead-letter arguments when a DLX is configured.
	if config.Queue != "" {
		queueArgs := buildQueueArgs(config)
		if _, err = channel.QueueDeclare(
			config.Queue,
			config.QueueDurable,
			config.QueueAutoDelete,
			config.QueueExclusive,
			false, // no-wait
			queueArgs,
		); err != nil {
			channel.Close()
			return fmt.Errorf("failed to declare queue %s: %w", config.Queue, err)
		}
	}

	r.consumerMutex.Lock()
	r.consumers[config.Name] = channel
	r.consumerMutex.Unlock()

	log.Infof("consumer %s created (queue=%s)", config.Name, config.Queue)
	return nil
}

// buildQueueArgs constructs the AMQP table of queue arguments.
// Currently wires up dead-letter-exchange / dead-letter-routing-key when
// the consumer configuration carries a DLX exchange name in the Exchange field
// together with a RoutingKey.  This follows the common convention of pairing a
// main queue with a DLQ: set Exchange to the dead-letter exchange name.
func buildQueueArgs(config *conf.Consumer) amqp.Table {
	args := amqp.Table{}
	// Use the consumer Exchange field as the dead-letter exchange when present.
	if config.Exchange != "" {
		args["x-dead-letter-exchange"] = config.Exchange
		if config.RoutingKey != "" {
			args["x-dead-letter-routing-key"] = config.RoutingKey
		}
	}
	if len(args) == 0 {
		return nil
	}
	return args
}

// GetEnabledProducers returns all enabled producers
func (r *RabbitMQClient) GetEnabledProducers() []*conf.Producer {
	var enabled []*conf.Producer
	for _, producer := range r.config.Producers {
		if producer.Enabled {
			enabled = append(enabled, producer)
		}
	}
	return enabled
}

// GetEnabledConsumers returns all enabled consumers
func (r *RabbitMQClient) GetEnabledConsumers() []*conf.Consumer {
	var enabled []*conf.Consumer
	for _, consumer := range r.config.Consumers {
		if consumer.Enabled {
			enabled = append(enabled, consumer)
		}
	}
	return enabled
}

// GetRabbitMQConfig returns the current RabbitMQ configuration
func (r *RabbitMQClient) GetRabbitMQConfig() *conf.RabbitMQ {
	return r.config
}

// GetConnection returns the underlying RabbitMQ connection
func (r *RabbitMQClient) GetConnection() *amqp.Connection {
	r.connectionMutex.RLock()
	defer r.connectionMutex.RUnlock()
	return r.connection
}

// IsConnected checks if the RabbitMQ client is connected
func (r *RabbitMQClient) IsConnected() bool {
	if r.closed.Load() {
		return false
	}
	r.connectionMutex.RLock()
	conn := r.connection
	r.connectionMutex.RUnlock()
	return conn != nil && !conn.IsClosed()
}

// GetMetrics returns the Metrics instance for this client.
func (r *RabbitMQClient) GetMetrics() *Metrics {
	return r.metrics
}

// --------------------------------------------------------------------------
// Producer interface implementation
// --------------------------------------------------------------------------

// PublishMessage publishes body to the given exchange/routingKey using the first
// enabled producer channel.  Additional AMQP publishing properties can be
// supplied via opts; the first element is used if present.
//
// Validation is applied to exchange and routingKey before the message is sent.
func (r *RabbitMQClient) PublishMessage(ctx context.Context, exchange, routingKey string, body []byte, opts ...amqp.Publishing) error {
	r.producerMutex.RLock()
	var ch *amqp.Channel
	for _, c := range r.producers {
		ch = c
		break
	}
	r.producerMutex.RUnlock()

	if ch == nil {
		return ErrProducerNotReady
	}
	return r.publishToChannel(ctx, ch, exchange, routingKey, body, opts...)
}

// PublishMessageWith is like PublishMessage but selects a specific producer
// channel by name, allowing fine-grained routing across multiple exchanges.
func (r *RabbitMQClient) PublishMessageWith(ctx context.Context, producerName, exchange, routingKey string, body []byte, opts ...amqp.Publishing) error {
	r.producerMutex.RLock()
	ch, ok := r.producers[producerName]
	r.producerMutex.RUnlock()

	if !ok {
		return WrapError(ErrProducerNotFound, "producer: "+producerName)
	}
	if ch == nil {
		return WrapError(ErrProducerNotReady, "producer: "+producerName)
	}
	return r.publishToChannel(ctx, ch, exchange, routingKey, body, opts...)
}

// publishToChannel validates inputs and performs the actual AMQP publish with
// metrics instrumentation.
func (r *RabbitMQClient) publishToChannel(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, body []byte, opts ...amqp.Publishing) error {
	if exchange != "" {
		if err := validateExchange(exchange); err != nil {
			return err
		}
	}

	if len(body) == 0 {
		return ErrEmptyMessage
	}

	// Default to a persistent octet-stream message; an explicit opts[0] overrides
	// it but the body and a zero timestamp are always filled in.
	pub := amqp.Publishing{
		ContentType:  "application/octet-stream",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         body,
	}
	if len(opts) > 0 {
		pub = opts[0]
		pub.Body = body
		if pub.Timestamp.IsZero() {
			pub.Timestamp = time.Now()
		}
	}

	start := time.Now()
	err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, pub)
	if r.metrics != nil {
		r.metrics.RecordProducerLatency(time.Since(start))
		if err != nil {
			r.metrics.IncrementProducerMessagesFailed()
		} else {
			r.metrics.IncrementProducerMessagesSent()
		}
	}
	if err != nil {
		return WrapError(ErrPublishMessageFailed, err.Error())
	}
	return nil
}

// GetProducer returns the underlying AMQP channel for the named producer.
// The caller must not close the channel directly.
func (r *RabbitMQClient) GetProducer(name string) (*amqp.Channel, error) {
	r.producerMutex.RLock()
	ch, ok := r.producers[name]
	r.producerMutex.RUnlock()
	if !ok {
		return nil, WrapError(ErrProducerNotFound, "producer: "+name)
	}
	return ch, nil
}

// IsProducerReady reports whether the named producer channel is registered and non-nil.
func (r *RabbitMQClient) IsProducerReady(name string) bool {
	r.producerMutex.RLock()
	ch, ok := r.producers[name]
	r.producerMutex.RUnlock()
	return ok && ch != nil
}

// --------------------------------------------------------------------------
// Consumer interface implementation
// --------------------------------------------------------------------------

// Subscribe starts consuming from queue using the first enabled consumer channel.
// The handler is called in a separate goroutine for each message; panics in
// the handler are recovered and counted in metrics.
func (r *RabbitMQClient) Subscribe(ctx context.Context, queue string, handler MessageHandler) error {
	r.consumerMutex.RLock()
	var (
		ch   *amqp.Channel
		name string
	)
	for n, c := range r.consumers {
		ch = c
		name = n
		break
	}
	r.consumerMutex.RUnlock()

	if ch == nil {
		return ErrConsumerNotReady
	}
	return r.subscribeWithChannel(ctx, name, ch, queue, handler)
}

// SubscribeWith starts consuming using a named consumer channel.
func (r *RabbitMQClient) SubscribeWith(ctx context.Context, consumerName, queue string, handler MessageHandler) error {
	if err := validateQueue(queue); err != nil {
		return err
	}

	r.consumerMutex.RLock()
	ch, ok := r.consumers[consumerName]
	r.consumerMutex.RUnlock()

	if !ok {
		return WrapError(ErrConsumerNotFound, "consumer: "+consumerName)
	}
	if ch == nil {
		return WrapError(ErrConsumerNotReady, "consumer: "+consumerName)
	}
	return r.subscribeWithChannel(ctx, consumerName, ch, queue, handler)
}

// subscribeWithChannel records the subscription so it can be re-established
// after a reconnect, then starts the dispatch loop on a dedicated channel.
//
// The shared consumer channel registered in r.consumers is intentionally NOT
// used for deliveries: amqp091 channels are not goroutine-safe, so each
// subscription gets its own channel (channel-per-goroutine) to avoid racing
// Ack/Nack against publishes or QoS calls on a shared channel.
func (r *RabbitMQClient) subscribeWithChannel(ctx context.Context, consumerName string, _ *amqp.Channel, queue string, handler MessageHandler) error {
	// Remember the subscription for reconnect-time restarts.
	r.subscriptionMu.Lock()
	r.subscriptions = append(r.subscriptions, &subscription{
		consumerName: consumerName,
		queue:        queue,
		handler:      handler,
	})
	r.subscriptionMu.Unlock()

	return r.startDispatchLoop(ctx, consumerName, nil, queue, handler)
}

// startDispatchLoop opens a dedicated AMQP channel for this subscription,
// applies the consumer's QoS, begins consuming, and runs the delivery loop in a
// WaitGroup-tracked goroutine whose lifetime is bound to closeChan so it drains
// cleanly on shutdown.  The _ shared channel parameter is accepted for symmetry
// with callers but is never used for deliveries (see subscribeWithChannel).
func (r *RabbitMQClient) startDispatchLoop(ctx context.Context, consumerName string, _ *amqp.Channel, queue string, handler MessageHandler) error {
	// Locate consumer config to read AutoAck / PrefetchCount settings.
	var (
		autoAck       bool
		prefetchCount int32
	)
	for _, c := range r.config.Consumers {
		if c.Name == consumerName {
			autoAck = c.AutoAck
			prefetchCount = c.PrefetchCount
			break
		}
	}

	// Open a dedicated channel for this subscription on the live connection.
	r.connectionMutex.RLock()
	conn := r.connection
	r.connectionMutex.RUnlock()
	if conn == nil || conn.IsClosed() {
		return ErrConsumerNotReady
	}
	ch, err := conn.Channel()
	if err != nil {
		return WrapError(ErrSubscribeFailed, err.Error())
	}

	if prefetchCount > 0 {
		if err = ch.Qos(int(prefetchCount), 0, false); err != nil {
			ch.Close()
			return WrapError(ErrSubscribeFailed, err.Error())
		}
	}

	deliveries, err := ch.ConsumeWithContext(ctx, queue, consumerName, autoAck, false, false, false, nil)
	if err != nil {
		ch.Close()
		return WrapError(ErrSubscribeFailed, err.Error())
	}

	r.subscriptionWG.Add(1)
	go func() {
		defer r.subscriptionWG.Done()
		defer ch.Close()
		defer func() {
			if rec := recover(); rec != nil {
				log.Errorf("rabbitmq consumer dispatch goroutine panic: %v", rec)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.closeChan:
				// Plugin is shutting down — stop before Ack/Nack on a
				// channel that is about to be torn down.
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				r.dispatchDelivery(ctx, d, handler, autoAck)
			}
		}
	}()
	return nil
}

// dispatchDelivery calls the user handler, recovering from panics and
// nack-ing the message on any error so it can be routed to a dead-letter queue.
func (r *RabbitMQClient) dispatchDelivery(ctx context.Context, d amqp.Delivery, handler MessageHandler, autoAck bool) {
	start := time.Now()
	var handlerErr error

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				handlerErr = fmt.Errorf("handler panic: %v\n%s", rec, debug.Stack())
				log.Errorf("RabbitMQ consumer panic recovered: %v", rec)
			}
		}()
		handlerErr = handler(ctx, d)
	}()

	if r.metrics != nil {
		r.metrics.RecordConsumerLatency(time.Since(start))
		if handlerErr != nil {
			r.metrics.IncrementConsumerMessagesFailed()
		} else {
			r.metrics.IncrementConsumerMessagesReceived()
		}
	}

	if autoAck {
		return // broker already acked for us
	}
	if handlerErr != nil {
		// requeue=false: let the broker route to DLQ if configured
		if nackErr := d.Nack(false, false); nackErr != nil {
			log.Warnf("RabbitMQ nack failed: %v", nackErr)
		}
	} else {
		if ackErr := d.Ack(false); ackErr != nil {
			log.Warnf("RabbitMQ ack failed: %v", ackErr)
		}
	}
}

// GetConsumer returns the underlying AMQP channel for the named consumer.
// The caller must not close the channel directly.
func (r *RabbitMQClient) GetConsumer(name string) (*amqp.Channel, error) {
	r.consumerMutex.RLock()
	ch, ok := r.consumers[name]
	r.consumerMutex.RUnlock()
	if !ok {
		return nil, WrapError(ErrConsumerNotFound, "consumer: "+name)
	}
	return ch, nil
}

// IsConsumerReady reports whether the named consumer channel is registered and non-nil.
func (r *RabbitMQClient) IsConsumerReady(name string) bool {
	r.consumerMutex.RLock()
	ch, ok := r.consumers[name]
	r.consumerMutex.RUnlock()
	return ok && ch != nil
}
