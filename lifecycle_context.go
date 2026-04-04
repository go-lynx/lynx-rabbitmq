package rabbitmq

import (
	"context"
	"fmt"

	"github.com/go-lynx/lynx/plugins"
)

func (r *RabbitMQClient) InitializeContext(ctx context.Context, plugin plugins.Plugin, rt plugins.Runtime) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rabbitmq initialize canceled before execution: %w", err)
	}
	return r.BasePlugin.Initialize(plugin, rt)
}

func (r *RabbitMQClient) StartContext(ctx context.Context, _ plugins.Plugin) error {
	return r.startupWithContext(ctx)
}

func (r *RabbitMQClient) StopContext(ctx context.Context, _ plugins.Plugin) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rabbitmq stop canceled before execution: %w", err)
	}
	return r.cleanupWithContext(ctx)
}

func (r *RabbitMQClient) IsContextAware() bool {
	return true
}

func (r *RabbitMQClient) PluginProtocol() plugins.PluginProtocol {
	protocol := r.BasePlugin.PluginProtocol()
	protocol.ContextLifecycle = true
	return protocol
}
