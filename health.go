package rabbitmq

import "fmt"

// CheckHealth verifies that the RabbitMQ client has established its core runtime state.
func (r *RabbitMQClient) CheckHealth() error {
	if r.connection == nil {
		return fmt.Errorf("rabbitmq connection not initialized")
	}
	if r.connection.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	if r.connectionManager == nil {
		return fmt.Errorf("rabbitmq connection manager not initialized")
	}
	if !r.connectionManager.IsConnected() {
		return fmt.Errorf("rabbitmq connection manager is not connected")
	}
	if r.healthChecker != nil && !r.healthChecker.IsHealthy() {
		return fmt.Errorf("rabbitmq health checker is unhealthy")
	}

	r.producerMutex.RLock()
	for name, channel := range r.producers {
		if channel == nil {
			r.producerMutex.RUnlock()
			return fmt.Errorf("rabbitmq producer %s is not ready", name)
		}
	}
	r.producerMutex.RUnlock()

	r.consumerMutex.RLock()
	for name, channel := range r.consumers {
		if channel == nil {
			r.consumerMutex.RUnlock()
			return fmt.Errorf("rabbitmq consumer %s is not ready", name)
		}
	}
	r.consumerMutex.RUnlock()

	return nil
}
