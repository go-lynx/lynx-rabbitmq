package rabbitmq

import (
	"github.com/go-lynx/lynx/pkg/factory"
	"github.com/go-lynx/lynx/plugins"
)

// init registers the RabbitMQ plugin with the global factory on import, keyed by
// plugin name and config prefix, so the plugin manager can discover and load it.
func init() {
	factory.GlobalTypedFactory().RegisterPlugin(pluginName, confPrefix, func() plugins.Plugin {
		return NewRabbitMQClient()
	})
}
