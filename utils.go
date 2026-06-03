// Package rabbitmq provides validation helpers and AMQP URL construction utilities.
package rabbitmq

import (
	"strings"
	"time"
)

// invalidNameChars lists characters that are not permitted in RabbitMQ resource names
// (exchanges, queues, virtual hosts).
var invalidNameChars = []string{"%", "&", "*", "+", "/", "\\", ":", "|", "<", ">", "?", " "}

// validateExchange returns an error if the exchange name is empty, too long, or contains
// characters that RabbitMQ rejects.
func validateExchange(exchange string) error {
	if exchange == "" {
		return ErrEmptyExchange
	}
	if len(exchange) > 255 {
		return WrapError(ErrInvalidExchange, "exchange name too long")
	}
	for _, char := range invalidNameChars {
		if strings.Contains(exchange, char) {
			return WrapError(ErrInvalidExchange, "exchange contains invalid character: "+char)
		}
	}
	return nil
}

// validateQueue returns an error if the queue name is empty, too long, or contains
// characters that RabbitMQ rejects.
func validateQueue(queue string) error {
	if queue == "" {
		return ErrEmptyQueue
	}
	if len(queue) > 255 {
		return WrapError(ErrInvalidQueue, "queue name too long")
	}
	for _, char := range invalidNameChars {
		if strings.Contains(queue, char) {
			return WrapError(ErrInvalidQueue, "queue contains invalid character: "+char)
		}
	}
	return nil
}

// validateExchangeType returns an error if the exchange type is not one of the four
// standard AMQP exchange types: direct, fanout, topic, headers.
func validateExchangeType(exchangeType string) error {
	switch exchangeType {
	case ExchangeTypeDirect, ExchangeTypeFanout, ExchangeTypeTopic, ExchangeTypeHeaders:
		return nil
	default:
		return WrapError(ErrInvalidExchangeType, "invalid exchange type: "+exchangeType)
	}
}

// invalidVHostChars lists characters that are not permitted in RabbitMQ virtual
// host names.  Unlike exchange/queue names, "/" is the standard default virtual
// host and must be allowed.
var invalidVHostChars = []string{"%", "&", "*", "+", "\\", ":", "|", "<", ">", "?", " "}

// validateVirtualHost returns an error if the virtual host name is empty, too long, or
// contains characters that RabbitMQ rejects.  The "/" character is explicitly
// permitted because it is the standard default virtual host.
func validateVirtualHost(vhost string) error {
	if vhost == "" {
		return ErrInvalidVirtualHost
	}
	if len(vhost) > 255 {
		return WrapError(ErrInvalidVirtualHost, "virtual host name too long")
	}
	for _, char := range invalidVHostChars {
		if strings.Contains(vhost, char) {
			return WrapError(ErrInvalidVirtualHost, "virtual host contains invalid character: "+char)
		}
	}
	return nil
}

// parseDuration parses a duration string and returns defaultDuration on any error or if
// the input string is empty.
func parseDuration(durationStr string, defaultDuration time.Duration) time.Duration {
	if durationStr == "" {
		return defaultDuration
	}
	d, err := time.ParseDuration(durationStr)
	if err != nil {
		return defaultDuration
	}
	return d
}

// buildAMQPURL constructs an amqp:// URL from its individual components.  Empty
// components fall back to the standard RabbitMQ defaults (guest/guest, localhost:5672, /).
func buildAMQPURL(username, password, host, port, vhost string) string {
	if username == "" {
		username = "guest"
	}
	if password == "" {
		password = "guest"
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5672"
	}
	if vhost == "" {
		vhost = "/"
	}
	return "amqp://" + username + ":" + password + "@" + host + ":" + port + "/" + vhost
}
