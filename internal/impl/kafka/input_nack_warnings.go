// Copyright 2026 Redpanda Data, Inc.
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

package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// NackMetricNameRedpanda is the name of the rejected messages counter
	// emitted by the Redpanda branded inputs.
	NackMetricNameRedpanda = "redpanda_nacks"

	// NackMetricNameKafka is the name of the rejected messages counter emitted
	// by the Kafka branded inputs.
	NackMetricNameKafka = "kafka_nacks"

	// NackWarningIntervalFieldName is the name of the field bounding how often
	// an input wrapped by WrapBatchInputNackWarnings logs a warning.
	NackWarningIntervalFieldName = "nack_warning_interval"
)

// NewNackWarningIntervalField returns the field read by
// WrapBatchInputNackWarnings. A partition stuck replaying the same batch
// produces one warning per interval instead of one per attempt.
func NewNackWarningIntervalField() *service.ConfigField {
	return service.NewDurationField(NackWarningIntervalFieldName).
		Description("The minimum time between two warnings about records rejected by the output. Rejections between warnings are summed up in the next warning and counted by the rejected records metric.").
		Default("10s").
		Advanced().
		Version("4.118.0")
}

// WrapBatchInputNackWarnings wraps a Kafka batch input so that batches
// rejected by the output are counted and reported.
//
// The Kafka readers cannot propagate a rejection to the broker. When
// auto_replay_nacks is enabled the auto retry wrapper replays a rejected batch
// with back off until it is delivered, so the offsets of its partition never
// advance and the consumer lag grows, without a single log line. When it is
// disabled the reader commits past the rejected records and they are lost.
// Either way the rejection is invisible to operators, hence this wrapper: it
// must sit outside the auto retry wrapper, so that it sees every rejection
// including the replays.
//
// Each rejected message increments a counter named metricName with topic and
// partition labels, and a warning is logged at most once every
// nack_warning_interval. The warning carries the number of consecutive
// rejections of the partition, which, for the ordered readers that keep one
// batch in flight per partition, is the number of times the same records were
// rejected.
func WrapBatchInputNackWarnings(conf *service.ParsedConfig, res *service.Resources, metricName string, rdr service.BatchInput) (service.BatchInput, error) {
	replay, err := conf.FieldBool(service.AutoRetryNacksToggleFieldName)
	if err != nil {
		return nil, err
	}
	interval, err := conf.FieldDuration(NackWarningIntervalFieldName)
	if err != nil {
		return nil, err
	}
	return newNackWarningInput(res, metricName, replay, interval, rdr), nil
}

func newNackWarningInput(res *service.Resources, metricName string, replay bool, warnInterval time.Duration, rdr service.BatchInput) *nackWarningInput {
	return &nackWarningInput{
		child:        rdr,
		log:          res.Logger(),
		nacks:        res.Metrics().NewCounter(metricName, "topic", "partition"),
		replay:       replay,
		warnInterval: warnInterval,
		now:          time.Now,
		consecutive:  map[topicPartition]int{},
	}
}

type topicPartition struct {
	topic     string
	partition string
}

type nackWarningInput struct {
	child service.BatchInput
	log   *service.Logger
	nacks *service.MetricCounter

	// replay is the value of auto_replay_nacks, which decides whether a
	// rejected batch is replayed or dropped and therefore what the warning
	// says.
	replay bool

	warnInterval time.Duration
	now          func() time.Time

	mut sync.Mutex
	// consecutive counts the rejections of each partition since its last
	// successful acknowledgement. Entries are removed on success, so the map
	// is bounded by the number of partitions with rejected batches.
	consecutive map[topicPartition]int
	// sinceWarn counts the messages rejected since the last warning.
	sinceWarn int64
	lastWarn  time.Time
}

func (n *nackWarningInput) Connect(ctx context.Context) error {
	return n.child.Connect(ctx)
}

func (n *nackWarningInput) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	t, ok := n.child.(service.ConnectionTestable)
	if !ok {
		return service.ConnectionTestNotSupported().AsList()
	}
	return t.ConnectionTest(ctx)
}

func (n *nackWarningInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batch, aFn, err := n.child.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}
	// The indexer tags the messages so that a batch error can be traced back
	// to the messages of this batch that failed. Index modifies the messages,
	// so work on a copy rather than on messages the child may still hold.
	batch = batch.Copy()
	idx := batch.Index()
	return batch, func(ctx context.Context, ackErr error) error {
		if ackErr == nil {
			n.observe(batchOrigin(batch), "", 0, nil)
		} else {
			tp, offset, size := rejectedOrigin(batch, idx, ackErr)
			n.observe(tp, offset, size, ackErr)
		}
		return aFn(ctx, ackErr)
	}, nil
}

func (n *nackWarningInput) Close(ctx context.Context) error {
	return n.child.Close(ctx)
}

// batchOrigin returns the topic and partition of the batch, as found in the
// metadata of its first message. The Kafka readers batch records of a single
// partition, so the first message identifies the partition of the batch.
func batchOrigin(batch service.MessageBatch) (tp topicPartition) {
	if len(batch) == 0 {
		return
	}
	tp.topic, _ = batch[0].MetaGet("kafka_topic")
	tp.partition, _ = batch[0].MetaGet("kafka_partition")
	return
}

// rejectedOrigin returns the partition of the batch, the offset of its first
// rejected message and the number of rejected messages. When ackErr is a batch
// error naming the messages that failed, only those are rejected: the auto
// retry wrapper replays them and nothing else. Any other error rejects the
// whole batch.
func rejectedOrigin(batch service.MessageBatch, idx *service.Indexer, ackErr error) (tp topicPartition, offset string, size int) {
	tp = batchOrigin(batch)
	if len(batch) == 0 {
		return
	}

	first, size := 0, len(batch)
	var bErr *service.BatchError
	if errors.As(ackErr, &bErr) && bErr.IndexedErrors() > 0 {
		failed := map[int]struct{}{}
		bErr.WalkMessagesIndexedBy(idx, func(i int, _ *service.Message, err error) bool {
			if err != nil && i >= 0 && i < len(batch) {
				failed[i] = struct{}{}
			}
			return true
		})
		// A batch error that cannot be traced back to this batch is treated
		// like a plain error, as the auto retry wrapper does.
		if len(failed) > 0 {
			size = len(failed)
			first = len(batch)
			for i := range failed {
				first = min(first, i)
			}
		}
	}
	offset, _ = batch[first].MetaGet("kafka_offset")
	return
}

func (n *nackWarningInput) observe(tp topicPartition, offset string, size int, ackErr error) {
	n.mut.Lock()
	defer n.mut.Unlock()

	if ackErr == nil {
		delete(n.consecutive, tp)
		return
	}

	n.nacks.Incr(int64(size), tp.topic, tp.partition)
	n.consecutive[tp]++
	n.sinceWarn += int64(size)

	now := n.now()
	if !n.lastWarn.IsZero() && now.Sub(n.lastWarn) < n.warnInterval {
		return
	}

	if n.replay {
		n.log.Warnf("Output rejected %v message(s) of topic %v partition %v starting at offset %v (%v consecutive rejection(s) on this partition, %v message(s) rejected since the last warning). Rejected messages are replayed until they are delivered, so this partition does not progress and its consumer lag grows. Fix the output error, route failed messages to a fallback output, or set auto_replay_nacks to false to drop them. Error: %v", size, tp.topic, tp.partition, offset, n.consecutive[tp], n.sinceWarn, ackErr)
	} else {
		n.log.Warnf("Output rejected %v message(s) of topic %v partition %v starting at offset %v (%v message(s) rejected since the last warning). auto_replay_nacks is false, so rejected messages are dropped and their offsets committed. Error: %v", size, tp.topic, tp.partition, offset, n.sinceWarn, ackErr)
	}
	n.lastWarn = now
	n.sinceWarn = 0
}

// nackWarningsDescription returns the documentation section describing how an
// input wrapped by WrapBatchInputNackWarnings reports rejected records,
// naming its counter metricName.
func nackWarningsDescription(metricName string) string {
	return `
== Rejected records

Kafka has no way to reject a record, so this input handles rejections itself. An output rejects a record when it cannot deliver it, or when a ` + "xref:components:outputs/reject_errored.adoc[`reject_errored`]" + ` output receives a record that failed its processors. When ` + "`auto_replay_nacks`" + ` is ` + "`true`" + `, which is the default, the input replays the rejected batch with back off until it is delivered. The offsets of that partition do not advance until then, so a persistent output error stalls the partition and its consumer lag grows. While this happens, the input logs a warning at most once per ` + "<<nack_warning_interval,`nack_warning_interval`>>" + ` and increments the ` + "`" + metricName + "`" + ` counter. The counter has ` + "`topic`" + ` and ` + "`partition`" + ` labels. To keep the pipeline flowing, route failed records to a ` + "xref:components:outputs/fallback.adoc[`fallback`]" + ` output, or set ` + "`auto_replay_nacks`" + ` to ` + "`false`" + ` to drop rejected records.
`
}
