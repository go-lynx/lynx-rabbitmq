package rabbitmq

const (
	// Plugin metadata
	pluginName        = "rabbitmq"
	pluginVersion     = "v1.6.0-beta"
	pluginDescription = "RabbitMQ message queue plugin for Lynx framework"
	confPrefix        = "rabbitmq"

	// Default values
	defaultDialTimeout     = "3s"
	defaultHeartbeat       = "30s"
	defaultPublishTimeout  = "3s"
	defaultRetryBackoff    = "100ms"
	defaultMaxRetries      = 3
	defaultPrefetchCount   = 1
	defaultMaxConcurrency  = 1
	defaultChannelPoolSize = 10
	defaultVirtualHost     = "/"

	// ExchangeTypeDirect Exchange types
	ExchangeTypeDirect  = "direct"
	ExchangeTypeFanout  = "fanout"
	ExchangeTypeTopic   = "topic"
	ExchangeTypeHeaders = "headers"

	// Default names
	defaultExchange = "lynx.exchange"
	defaultQueue    = "lynx.queue"
	defaultConsumer = "lynx.consumer"
)
