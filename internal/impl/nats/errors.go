// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
