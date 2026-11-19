// Copyright 2026 Redpanda Data, Inc.
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
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
)

const (
	// lagMetricNameRedpanda is the consumer lag gauge emitted by the
	// Redpanda branded inputs (`redpanda`, `redpanda_common` and the migrator).
	lagMetricNameRedpanda = "redpanda_lag"

	// lagMetricNameKafka is the consumer lag gauge emitted by the Kafka branded
	// inputs (`kafka_franz`, `ockam_kafka`).
	lagMetricNameKafka = "kafka_lag"

	// InputPendingMetricName is the gauge an input sets to the number of
	// messages it knows remain at its source. It is optional and shared across
	// inputs: any input able to estimate its backlog sets it under this name,
	// with no labels of its own, so the component label and path identify the
	// series. An input sets it to zero when it stops consuming. The Kafka
	// family sets it to the lag of this member's assigned partitions on each
	// refresh and leaves it untouched while the group is not stable, so the
	// value can be a refresh period behind.
	InputPendingMetricName = "input_pending"
)

// partitionKey identifies a topic partition.
type partitionKey struct {
	topic     string
	partition int32
}

// lagSnapshot is the consumer group's lag as of one refresh.
type lagSnapshot struct {
	byPartition map[partitionKey]int64
	// total is the lag of the partitions assigned to this member, and
	// totalKnown is false while the group is rebalancing, before this client
	// has joined, or when one of this member's partitions has an offset error.
	total      int64
	totalKnown bool
}

// groupLagFetcher fetches the lag of a consumer group.
type groupLagFetcher func(ctx context.Context) (kadm.DescribedGroupLags, error)

// gaugeSetter is the part of a gauge the lag tracker uses. A gauge with no
// labels is set with no label values.
type gaugeSetter interface {
	Set(int64, ...string)
}

// ConsumerLag periodically fetches the lag of a consumer group, publishes it
// per partition as a gauge, sets the pending gauge to this member's share, and
// keeps the latest refresh for cheap reads.
type ConsumerLag struct {
	lagUpdater    *asyncroutine.Periodic
	fetch         groupLagFetcher
	memberID      func() string
	logger        *service.Logger
	gauge         gaugeSetter
	pending       gaugeSetter
	refreshPeriod time.Duration
	snapshot      atomic.Pointer[lagSnapshot]
}

// NewConsumerLag creates a new ConsumerLag instance.
func NewConsumerLag(
	client *kgo.Client,
	consumerGroup string,
	logger *service.Logger,
	topicLagGauge, pendingGauge *service.MetricGauge,
	topicLagRefreshPeriod time.Duration,
) *ConsumerLag {
	adminClient := kadm.NewClient(client)
	return newConsumerLag(func(ctx context.Context) (kadm.DescribedGroupLags, error) {
		return adminClient.Lag(ctx, consumerGroup)
	}, func() string {
		id, _ := client.GroupMetadata()
		return id
	}, logger, topicLagGauge, pendingGauge, topicLagRefreshPeriod)
}

func newConsumerLag(
	fetch groupLagFetcher,
	memberID func() string,
	logger *service.Logger,
	topicLagGauge gaugeSetter,
	pendingGauge gaugeSetter,
	topicLagRefreshPeriod time.Duration,
) *ConsumerLag {
	cl := &ConsumerLag{
		fetch:         fetch,
		memberID:      memberID,
		logger:        logger,
		gauge:         topicLagGauge,
		pending:       pendingGauge,
		refreshPeriod: topicLagRefreshPeriod,
	}
	cl.lagUpdater = asyncroutine.NewPeriodicRunOnStart(topicLagRefreshPeriod, cl.refreshWithTimeout)
	return cl
}

func (cl *ConsumerLag) refreshWithTimeout(ctx context.Context) {
	ctx, done := context.WithTimeout(ctx, cl.refreshPeriod)
	defer done()
	cl.refresh(ctx)
}

// refresh fetches the group's lag once. On failure, or when the group reports
// a describe or offset fetch error, the previous snapshot and gauges are kept,
// so readers see the last known lag rather than nothing.
func (cl *ConsumerLag) refresh(ctx context.Context) {
	me := cl.memberID()
	lags, err := cl.fetch(ctx)
	if err == nil {
		err = lags.Error()
	}
	if err != nil {
		cl.logger.Debugf("Failed to fetch group lags: %s", err)
		return
	}
	// A member ID that changed during the fetch means the described
	// assignments are not ours any more.
	if cl.memberID() != me {
		me = ""
	}
	cl.apply(lags, me)
}

// groupStateStable is the state of a consumer group whose members all hold
// their assignments.
const groupStateStable = "Stable"

// apply replaces the snapshot with the given group lag and publishes each
// partition's lag to the gauge. A partition with an offset error, such as a
// committed offset on a deleted topic, is left out, and a partition that is
// out of the snapshot after being in it is published as zero, so one rule
// covers both it and a partition that has left the group.
//
// The total counts only the partitions assigned to this member, so instances
// sharing a group each report their own share. It is unknown while the group
// is not stable, while this client is not among the described members, which
// covers the time before its join completes, or when one of this member's
// partitions had an error, since a confident zero in those states would read
// as caught up. The pending gauge is set only when the total is known and so
// keeps its last value otherwise.
func (cl *ConsumerLag) apply(lags kadm.DescribedGroupLags, me string) {
	snap := &lagSnapshot{byPartition: map[partitionKey]int64{}, totalKnown: me != ""}
	for _, gl := range lags {
		if gl.State != groupStateStable || !slices.ContainsFunc(gl.Members, func(m kadm.DescribedGroupMember) bool {
			return m.MemberID == me
		}) {
			snap.totalKnown = false
		}
		for _, tl := range gl.Lag {
			for _, pl := range tl {
				mine := pl.Member != nil && pl.Member.MemberID == me
				if pl.Err != nil {
					snap.totalKnown = snap.totalKnown && !mine
					continue
				}
				lag := max(pl.Lag, 0)
				cl.gauge.Set(lag, pl.Topic, strconv.Itoa(int(pl.Partition)))
				snap.byPartition[partitionKey{pl.Topic, pl.Partition}] = lag
				if mine {
					snap.total += lag
				}
			}
		}
	}
	if prev := cl.snapshot.Load(); prev != nil {
		for pk := range prev.byPartition {
			if _, still := snap.byPartition[pk]; !still {
				cl.gauge.Set(0, pk.topic, strconv.Itoa(int(pk.partition)))
			}
		}
	}
	cl.snapshot.Store(snap)
	if snap.totalKnown {
		cl.pending.Set(snap.total)
	}
}

// Start starts the lag updater. The first refresh runs straight away rather
// than one period in. Start is called before the client has joined the group,
// so that refresh publishes the per partition lag but rarely a known total;
// the pending gauge usually gets its first value one refresh period later.
func (cl *ConsumerLag) Start() {
	cl.lagUpdater.Start()
}

// Stop stops the lag updater and zeroes the pending gauge, as nothing is
// pending for a session that has ended.
func (cl *ConsumerLag) Stop() {
	cl.lagUpdater.Stop()
	cl.pending.Set(0)
}

// Load loads the consumer lag for a given topic and partition as of the
// latest refresh, or zero when unknown.
func (cl *ConsumerLag) Load(topic string, partition int32) int64 {
	snap := cl.snapshot.Load()
	if snap == nil {
		return 0
	}
	return snap.byPartition[partitionKey{topic, partition}]
}
