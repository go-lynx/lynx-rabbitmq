package rabbitmq

import "github.com/go-lynx/lynx/log"

const (
	sharedPluginResourceName     = pluginName + ".plugin"
	sharedReadinessResourceName  = pluginName + ".readiness"
	sharedHealthResourceName     = pluginName + ".health"
	privateReadinessResourceName = "readiness"
	privateHealthResourceName    = "health"
)

func (r *RabbitMQClient) registerRuntimePluginAlias() {
	if r == nil || r.rt == nil {
		return
	}
	if err := r.rt.RegisterSharedResource(sharedPluginResourceName, r); err != nil {
		log.Warnf("failed to register RabbitMQ shared plugin alias: %v", err)
	}
}

func (r *RabbitMQClient) publishRuntimeContract(ready, healthy bool) {
	if r == nil || r.rt == nil {
		return
	}
	for _, item := range []struct {
		name  string
		value any
	}{
		{name: sharedReadinessResourceName, value: ready},
		{name: sharedHealthResourceName, value: healthy},
	} {
		if err := r.rt.RegisterSharedResource(item.name, item.value); err != nil {
			log.Warnf("failed to register RabbitMQ shared runtime contract %s: %v", item.name, err)
		}
	}
	if err := r.rt.RegisterPrivateResource(privateReadinessResourceName, ready); err != nil {
		log.Warnf("failed to register RabbitMQ private readiness resource: %v", err)
	}
	if err := r.rt.RegisterPrivateResource(privateHealthResourceName, healthy); err != nil {
		log.Warnf("failed to register RabbitMQ private health resource: %v", err)
	}
}
