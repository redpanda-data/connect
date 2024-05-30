package nats

import (
	"github.com/nats-io/nats.go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func errorHandlerOption(logger *service.Logger) nats.Option {
	return nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		if nc != nil {
			logger = logger.With("connection-status", nc.Status())
		}
		if sub != nil {
			logger = logger.With("subject", sub.Subject)
			if c, err := sub.ConsumerInfo(); err == nil {
				logger = logger.With("consumer", c.Name)
			}
		}
		logger.Errorf("nats operation failed: %v\n", err)
	})
}
