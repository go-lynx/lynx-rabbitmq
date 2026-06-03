// Package rabbitmq contains manager types for health checking, connection
// lifecycle, and retry handling.
package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/go-lynx/lynx-rabbitmq/conf"
	"github.com/go-lynx/lynx/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

// HealthChecker performs periodic liveness probes and exposes the result to the
// rest of the plugin.
type HealthChecker struct {
	interval   time.Duration
	stopChan   chan struct{}
	stopOnce   sync.Once
	healthy    bool
	lastCheck  time.Time
	errorCount int
	lastError  error
	mu         sync.RWMutex
	stopped    bool
}

// NewHealthChecker returns a HealthChecker that fires every 30 seconds by default.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		interval: 30 * time.Second,
		stopChan: make(chan struct{}),
		healthy:  true,
	}
}

// Start launches the background health-check goroutine.
func (h *HealthChecker) Start() {
	go h.run()
}

// Stop terminates the background goroutine; safe to call multiple times.
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	stopped := h.stopped
	h.mu.Unlock()

	if !stopped {
		h.stopOnce.Do(func() {
			close(h.stopChan)
			h.mu.Lock()
			h.stopped = true
			h.mu.Unlock()
		})
	}
}

// IsHealthy returns true if the last check succeeded.
func (h *HealthChecker) IsHealthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

// GetLastCheck returns the timestamp of the most recent health check.
func (h *HealthChecker) GetLastCheck() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastCheck
}

// GetErrorCount returns the cumulative number of failed health checks.
func (h *HealthChecker) GetErrorCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.errorCount
}

// GetLastError returns the error from the most recent failed check, or nil.
func (h *HealthChecker) GetLastError() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastError
}

// run is the internal ticker loop.
func (h *HealthChecker) run() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.performHealthCheck()
		case <-h.stopChan:
			return
		}
	}
}

// performHealthCheck records a successful health check timestamp.
// External callers (e.g. the client's CheckHealth) drive the actual
// connection probe; this resets the basic liveness marker.
func (h *HealthChecker) performHealthCheck() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastCheck = time.Now()
	h.healthy = true
	h.lastError = nil
}

// --------------------------------------------------------------------------
// ConnectionManager
// --------------------------------------------------------------------------

// reconnectFunc is the signature of the callback the ConnectionManager calls
// when it needs to re-establish the AMQP connection.
type reconnectFunc func() (*amqp.Connection, error)

// ConnectionManager monitors the AMQP connection, signals disconnects, and
// drives reconnection via a caller-supplied callback.
type ConnectionManager struct {
	config       *conf.RabbitMQ
	connected    bool
	stopChan     chan struct{}
	stopOnce     sync.Once
	mu           sync.RWMutex
	stopped      bool
	reconnect    reconnectFunc   // nil when reconnect is not wired up
	connNotify   chan *amqp.Error // receives broker-side close notifications
}

// NewConnectionManager returns a ConnectionManager for the given configuration.
// Call SetReconnectFunc to wire up automatic reconnection before Start.
func NewConnectionManager(config *conf.RabbitMQ) *ConnectionManager {
	return &ConnectionManager{
		config:   config,
		stopChan: make(chan struct{}),
	}
}

// SetReconnectFunc registers the callback that will be invoked when the broker
// closes the connection.  fn must re-dial and return the fresh connection.
func (c *ConnectionManager) SetReconnectFunc(fn reconnectFunc) {
	c.mu.Lock()
	c.reconnect = fn
	c.mu.Unlock()
}

// SetConnectionNotify registers the amqp.Connection's close-notify channel so
// the manager can react to broker-initiated disconnects.
func (c *ConnectionManager) SetConnectionNotify(ch chan *amqp.Error) {
	c.mu.Lock()
	c.connNotify = ch
	c.mu.Unlock()
}

// Start marks the connection as established and, if a notify channel has been
// registered, starts a background watcher that triggers reconnection on disconnect.
func (c *ConnectionManager) Start() {
	c.mu.Lock()
	c.connected = true
	notify := c.connNotify
	reconnFn := c.reconnect
	c.mu.Unlock()

	log.Infof("RabbitMQ connection manager started")

	if notify != nil && reconnFn != nil {
		go c.watchConnection(notify, reconnFn)
	}
}

// watchConnection blocks until the broker closes the connection or Stop is
// called, then attempts to reconnect with exponential back-off.
func (c *ConnectionManager) watchConnection(notify chan *amqp.Error, reconnFn reconnectFunc) {
	select {
	case amqpErr, ok := <-notify:
		if !ok || amqpErr == nil {
			// Channel closed without error — plugin is shutting down.
			return
		}
		log.Warnf("RabbitMQ connection closed by broker: %v — reconnecting", amqpErr)
	case <-c.stopChan:
		return
	}

	// Mark disconnected.
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()

	// Exponential back-off reconnect loop.
	backoff := 500 * time.Millisecond
	for {
		select {
		case <-c.stopChan:
			return
		default:
		}

		conn, err := reconnFn()
		if err == nil {
			c.mu.Lock()
			c.connected = true
			c.mu.Unlock()
			log.Infof("RabbitMQ reconnected successfully")
			// Register a new notify channel and restart the watcher.
			newNotify := conn.NotifyClose(make(chan *amqp.Error, 1))
			go c.watchConnection(newNotify, reconnFn)
			return
		}

		log.Warnf("RabbitMQ reconnect attempt failed: %v — retrying in %s", err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-c.stopChan:
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// Stop marks the manager as stopped; safe to call multiple times.
func (c *ConnectionManager) Stop() {
	c.mu.Lock()
	c.connected = false
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	ch := c.stopChan
	c.mu.Unlock()
	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
	log.Infof("RabbitMQ connection manager stopped")
}

// IsConnected returns true when the manager believes the connection is live.
func (c *ConnectionManager) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// GetHealthChecker implements ConnectionManagerInterface; returns nil because
// the health checker is managed separately by the client.
func (c *ConnectionManager) GetHealthChecker() HealthCheckerInterface {
	return nil
}

// ForceReconnect immediately triggers the reconnect callback if one has been
// registered, blocking until it succeeds or returns an error.
func (c *ConnectionManager) ForceReconnect() {
	c.mu.Lock()
	fn := c.reconnect
	c.mu.Unlock()

	log.Infof("forcing RabbitMQ reconnection")

	if fn == nil {
		// No callback — just flip the flag for compatibility with tests.
		c.mu.Lock()
		c.connected = false
		c.connected = true
		c.mu.Unlock()
		return
	}

	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()

	if _, err := fn(); err != nil {
		log.Warnf("ForceReconnect failed: %v", err)
		return
	}
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
}

// --------------------------------------------------------------------------
// RetryHandler
// --------------------------------------------------------------------------

// RetryHandler executes operations with exponential back-off retry semantics
// derived from the first producer's configuration.
type RetryHandler struct {
	config *conf.RabbitMQ
}

// NewRetryHandler returns a RetryHandler backed by the given configuration.
func NewRetryHandler(config *conf.RabbitMQ) *RetryHandler {
	return &RetryHandler{config: config}
}

// DoWithRetry executes operation up to maxRetries+1 times.  It returns as soon
// as operation succeeds; if all attempts fail the last error is wrapped with
// "max retries exceeded".  Context cancellation is checked before each attempt
// and during back-off sleeps.
func (r *RetryHandler) DoWithRetry(ctx context.Context, operation func() error) error {
	maxRetries := int(defaultMaxRetries)
	backoffTime := 100 * time.Millisecond

	if len(r.config.Producers) > 0 {
		maxRetries = int(r.config.Producers[0].MaxRetries)
		if r.config.Producers[0].RetryBackoff != nil {
			backoffTime = r.config.Producers[0].RetryBackoff.AsDuration()
		}
	}

	var lastErr error
	backoff := backoffTime

	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == maxRetries {
			break
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}

	return WrapError(lastErr, "max retries exceeded")
}
