package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	closed            bool
	metrics           *Metrics
	healthChecker     *HealthChecker
	connectionManager *ConnectionManager
	retryHandler      *RetryHandler
	goroutinePool     *GoroutinePool
}

// NewRabbitMQClient creates a new RabbitMQ client plugin instance
func NewRabbitMQClient() *RabbitMQClient {
	c := &RabbitMQClient{
		config:    defaultRabbitMQConfig(),
		producers: make(map[string]*amqp.Channel),
		consumers: make(map[string]*amqp.Channel),
		closeChan: make(chan struct{}),
		closed:    false,
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

// InitializeResources initializes the plugin with configuration
func (r *RabbitMQClient) InitializeResources(rt plugins.Runtime) error {
	// Initialize base plugin
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

	// Initialize managers
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

// StartupTasks initializes RabbitMQ client and performs health check
func (r *RabbitMQClient) StartupTasks() error {
	return r.startupWithContext(context.Background())
}

// CleanupTasks gracefully shuts down the plugin
func (r *RabbitMQClient) CleanupTasks() error {
	return r.cleanupWithContext(context.Background())
}

func (r *RabbitMQClient) startupWithContext(ctx context.Context) error {
	log.Infof("initializing RabbitMQ client")
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rabbitmq startup canceled before execution: %w", err)
	}
	r.publishRuntimeContract(false, false)

	// Connect to RabbitMQ
	if err := r.connect(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("rabbitmq startup canceled after connect: %w", err)
	}

	// Initialize producers
	if err := r.initializeProducers(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize producers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("rabbitmq startup canceled after producer init: %w", err)
	}

	// Initialize consumers
	if err := r.initializeConsumers(); err != nil {
		r.publishRuntimeContract(false, false)
		return fmt.Errorf("failed to initialize consumers: %w", err)
	}

	// Start health checker
	if r.healthChecker != nil {
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
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
	r.closed = true

	// Stop health checker
	if r.healthChecker != nil {
		r.healthChecker.Stop()
	}

	// Stop connection manager
	if r.connectionManager != nil {
		r.connectionManager.Stop()
	}

	// Stop goroutine pool
	if r.goroutinePool != nil {
		r.goroutinePool.Wait()
	}

	// Close consumers
	r.consumerMutex.Lock()
	for name, channel := range r.consumers {
		if channel != nil {
			channel.Close()
			log.Infof("consumer channel %s closed", name)
		}
	}
	r.consumers = make(map[string]*amqp.Channel)
	r.consumerMutex.Unlock()

	// Close producers
	r.producerMutex.Lock()
	for name, channel := range r.producers {
		if channel != nil {
			channel.Close()
			log.Infof("producer channel %s closed", name)
		}
	}
	r.producers = make(map[string]*amqp.Channel)
	r.producerMutex.Unlock()

	// Close connection
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

// connect establishes connection to RabbitMQ
func (r *RabbitMQClient) connect() error {
	if len(r.config.Urls) == 0 {
		return fmt.Errorf("no RabbitMQ URLs configured")
	}

	// Use the first URL for now (could be extended to support multiple URLs)
	url := r.config.Urls[0]

	// Build connection options
	config := amqp.Config{
		Vhost:     r.config.VirtualHost,
		Heartbeat: r.config.Heartbeat.AsDuration(),
		Locale:    "en_US",
	}

	// Set authentication if provided
	if r.config.Username != "" && r.config.Password != "" {
		// URL should already contain credentials, but we can set them explicitly if needed
	}

	// Connect to RabbitMQ
	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ at %s: %w", url, err)
	}

	r.connectionMutex.Lock()
	r.connection = conn
	r.connectionMutex.Unlock()

	log.Infof("connected to RabbitMQ at %s", url)
	return nil
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

// createProducer creates a RabbitMQ producer channel
func (r *RabbitMQClient) createProducer(config *conf.Producer) error {
	channel, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}

	// Declare exchange if configured
	if config.Exchange != "" {
		err = channel.ExchangeDeclare(
			config.Exchange,
			config.ExchangeType,
			config.ExchangeDurable,
			false, // auto-delete
			false, // internal
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			channel.Close()
			return fmt.Errorf("failed to declare exchange %s: %w", config.Exchange, err)
		}
	}

	r.producerMutex.Lock()
	r.producers[config.Name] = channel
	r.producerMutex.Unlock()

	log.Infof("producer %s created", config.Name)
	return nil
}

// createConsumer creates a RabbitMQ consumer channel
func (r *RabbitMQClient) createConsumer(config *conf.Consumer) error {
	channel, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}

	// Set QoS if configured
	if config.PrefetchCount > 0 {
		err = channel.Qos(int(config.PrefetchCount), 0, false)
		if err != nil {
			channel.Close()
			return fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	// Declare queue if configured
	if config.Queue != "" {
		_, err = channel.QueueDeclare(
			config.Queue,
			config.QueueDurable,
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			channel.Close()
			return fmt.Errorf("failed to declare queue %s: %w", config.Queue, err)
		}
	}

	r.consumerMutex.Lock()
	r.consumers[config.Name] = channel
	r.consumerMutex.Unlock()

	log.Infof("consumer %s created", config.Name)
	return nil
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
	return !r.closed && r.connection != nil && !r.connection.IsClosed()
}
