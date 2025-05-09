// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

// ConsumerLag is a struct that manages the consumer lag for Kafka topics.
type ConsumerLag struct {
	lagUpdater    *asyncroutine.Periodic
	topicLagCache *sync.Map
}

// NewConsumerLag creates a new ConsumerLag instance.
func NewConsumerLag(
	client *kgo.Client,
	consumerGroup string,
	logger *service.Logger,
	topicLagGauge *service.MetricGauge,
	topicLagRefreshPeriod time.Duration,
) *ConsumerLag {
	adminClient := kadm.NewClient(client)
	topicLagCache := new(sync.Map)
	lagUpdater := asyncroutine.NewPeriodicWithContext(topicLagRefreshPeriod, func(ctx context.Context) {
		ctx, done := context.WithTimeout(ctx, topicLagRefreshPeriod)
		defer done()
		lags, err := adminClient.Lag(ctx, consumerGroup)
		if err != nil {
			logger.Debugf("Failed to fetch group lags: %s", err)
			return
		}
		lags.Each(func(gl kadm.DescribedGroupLag) {
			for _, gl := range gl.Lag {
				for _, pl := range gl {
					lag := max(pl.Lag, 0)
					topicLagGauge.Set(lag, pl.Topic, strconv.Itoa(int(pl.Partition)))
					topicLagCache.Store(fmt.Sprintf("%s_%d", pl.Topic, pl.Partition), lag)
				}
			}
		})
	})
	return &ConsumerLag{
		lagUpdater:    lagUpdater,
		topicLagCache: topicLagCache,
	}
}

// Start starts the lag updater.
func (cl *ConsumerLag) Start() {
	cl.lagUpdater.Start()
}

// Stop stops the lag updater.
func (cl *ConsumerLag) Stop() {
	cl.lagUpdater.Stop()
}

// Load loads the consumer lag for a given topic and partition.
func (cl *ConsumerLag) Load(topic string, partition int32) int64 {
	lag := int64(0)
	if val, ok := cl.topicLagCache.Load(fmt.Sprintf("%s_%d", topic, partition)); ok {
		lag = val.(int64)
	}
	return lag
}
