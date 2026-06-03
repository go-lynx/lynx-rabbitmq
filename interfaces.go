// Package rabbitmq declares the core interfaces that the RabbitMQ plugin exposes
// to application code.
package rabbitmq

import (
	"context"
	"time"

	"github.com/go-lynx/lynx/plugins"
	amqp "github.com/rabbitmq/amqp091-go"
)

// MessageHandler is the function signature that consumer callbacks must implement.
// A non-nil error causes the message to be nack'd; a nil error causes an ack.
type MessageHandler func(ctx context.Context, msg amqp.Delivery) error

// Producer is the interface for sending messages to RabbitMQ exchanges.
type Producer interface {
	// PublishMessage publishes body to the default (first) producer's exchange.
	PublishMessage(ctx context.Context, exchange, routingKey string, body []byte, opts ...amqp.Publishing) error

	// PublishMessageWith selects a named producer channel before publishing.
	PublishMessageWith(ctx context.Context, producerName, exchange, routingKey string, body []byte, opts ...amqp.Publishing) error

	// GetProducer returns the underlying AMQP channel for the named producer.
	GetProducer(name string) (*amqp.Channel, error)

	// IsProducerReady reports whether the named producer channel is live.
	IsProducerReady(name string) bool
}

// Consumer is the interface for receiving messages from RabbitMQ queues.
type Consumer interface {
	// Subscribe starts consuming from queue using the default consumer channel.
	Subscribe(ctx context.Context, queue string, handler MessageHandler) error

	// SubscribeWith starts consuming using a named consumer channel.
	SubscribeWith(ctx context.Context, consumerName, queue string, handler MessageHandler) error

	// GetConsumer returns the underlying AMQP channel for the named consumer.
	GetConsumer(name string) (*amqp.Channel, error)

	// IsConsumerReady reports whether the named consumer channel is live.
	IsConsumerReady(name string) bool
}

// ClientInterface is the full public interface of the RabbitMQ plugin.
type ClientInterface interface {
	Producer
	Consumer

	// InitializeResources wires up configuration and sub-managers.
	InitializeResources(rt plugins.Runtime) error

	// StartupTasks connects to the broker and starts all producers/consumers.
	StartupTasks() error

	// ShutdownTasks gracefully closes all channels and the connection.
	ShutdownTasks() error

	// GetMetrics returns the live metrics instance.
	GetMetrics() *Metrics
}

// MetricsProvider is a read-only view of plugin metrics.
type MetricsProvider interface {
	// GetStats returns a snapshot of all counters and timestamps.
	GetStats() map[string]any

	// Reset zeroes all counters.
	Reset()
}

// HealthCheckerInterface is the contract for the periodic liveness probe.
type HealthCheckerInterface interface {
	// Start launches the background check goroutine.
	Start()

	// Stop terminates the background check goroutine.
	Stop()

	// IsHealthy returns true when the last check succeeded.
	IsHealthy() bool

	// GetLastCheck returns the timestamp of the most recent check.
	GetLastCheck() time.Time

	// GetErrorCount returns the cumulative number of failed checks.
	GetErrorCount() int
}

// ConnectionManagerInterface is the contract for connection lifecycle management.
type ConnectionManagerInterface interface {
	// Start marks the connection as live and begins watching for disconnects.
	Start()

	// Stop terminates the watcher and marks the connection closed.
	Stop()

	// IsConnected returns true when the manager believes the connection is live.
	IsConnected() bool

	// GetHealthChecker returns the associated health checker, if any.
	GetHealthChecker() HealthCheckerInterface

	// ForceReconnect synchronously re-dials the broker.
	ForceReconnect()
}

// RetryHandlerInterface executes operations with configurable retry back-off.
type RetryHandlerInterface interface {
	// DoWithRetry runs operation, retrying on failure up to the configured maximum.
	DoWithRetry(ctx context.Context, operation func() error) error
}

// GoroutinePoolInterface manages a bounded pool of reusable goroutines.
type GoroutinePoolInterface interface {
	// Submit enqueues a task for execution in the pool.
	Submit(task func())

	// Wait closes the pool and blocks until all tasks have finished.
	Wait()
}
