// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Metrics contains counters and timers for tracking Spanner CDC operations.
type Metrics struct {
	// partitionRecordCreatedCount tracks the total number of partitions created
	// during connector execution.
	partitionRecordCreatedCount *service.MetricCounter
	// partitionRecordRunningCount tracks the total number of partitions that
	// have started processing.
	partitionRecordRunningCount *service.MetricCounter
	// partitionRecordFinishedCount tracks the total number of partitions that
	// have completed processing.
	partitionRecordFinishedCount *service.MetricCounter
	// partitionRecordSplitCount tracks the total number of partition splits
	// identified during execution.
	partitionRecordSplitCount *service.MetricCounter
	// partitionRecordMergeCount tracks the total number of partition merges
	// identified during execution.
	partitionRecordMergeCount *service.MetricCounter
	// partitionCreatedToScheduled measures time (ns) for partitions to
	// transition from CREATED to SCHEDULED state.
	partitionCreatedToScheduled *service.MetricTimer
	// partitionScheduledToRunning measures time (ns) for partitions to
	// transition from SCHEDULED to RUNNING state.
	partitionScheduledToRunning *service.MetricTimer
	// queryCount tracks the total number of queries issued to Spanner during
	// connector execution.
	queryCount *service.MetricCounter
	// dataChangeRecordCount tracks the total number of data change records processed.
	dataChangeRecordCount *service.MetricCounter
	// dataChangeRecordCommittedToEmitted counts records processing latency.
	dataChangeRecordCommittedToEmitted *service.MetricTimer
	// heartbeatRecordCount tracks the total number of heartbeat records received.
	heartbeatRecordCount *service.MetricCounter

	streamID string
}

const metricsStreamIDLabel = "stream"

// NewMetrics creates a new Metrics instance using the provided service Metrics.
func NewMetrics(m *service.Metrics, streamID string) *Metrics {
	return &Metrics{
		partitionRecordCreatedCount:        m.NewCounter("spanner_cdc_partition_record_created_count", metricsStreamIDLabel),
		partitionRecordRunningCount:        m.NewCounter("spanner_cdc_partition_record_running_count", metricsStreamIDLabel),
		partitionRecordFinishedCount:       m.NewCounter("spanner_cdc_partition_record_finished_count", metricsStreamIDLabel),
		partitionRecordSplitCount:          m.NewCounter("spanner_cdc_partition_record_split_count", metricsStreamIDLabel),
		partitionRecordMergeCount:          m.NewCounter("spanner_cdc_partition_record_merge_count", metricsStreamIDLabel),
		partitionCreatedToScheduled:        m.NewTimer("spanner_cdc_partition_created_to_scheduled_ns", metricsStreamIDLabel),
		partitionScheduledToRunning:        m.NewTimer("spanner_cdc_partition_scheduled_to_running_ns", metricsStreamIDLabel),
		queryCount:                         m.NewCounter("spanner_cdc_query_count", metricsStreamIDLabel),
		dataChangeRecordCount:              m.NewCounter("spanner_cdc_data_change_record_count", metricsStreamIDLabel),
		dataChangeRecordCommittedToEmitted: m.NewTimer("spanner_cdc_data_change_record_committed_to_emitted_ns", metricsStreamIDLabel),
		heartbeatRecordCount:               m.NewCounter("spanner_cdc_heartbeat_record_count", metricsStreamIDLabel),

		streamID: streamID,
	}
}

// IncPartitionRecordCreatedCount increments the partition record created counter.
func (m *Metrics) IncPartitionRecordCreatedCount(n int) {
	m.partitionRecordCreatedCount.Incr(int64(n), m.streamID)
}

// IncPartitionRecordRunningCount increments the partition record running counter.
func (m *Metrics) IncPartitionRecordRunningCount() {
	m.partitionRecordRunningCount.Incr(1, m.streamID)
}

// IncPartitionRecordFinishedCount increments the partition record finished counter.
func (m *Metrics) IncPartitionRecordFinishedCount() {
	m.partitionRecordFinishedCount.Incr(1, m.streamID)
}

// IncPartitionRecordSplitCount increments the partition record split counter.
func (m *Metrics) IncPartitionRecordSplitCount() {
	m.partitionRecordSplitCount.Incr(1, m.streamID)
}

// IncPartitionRecordMergeCount increments the partition record merge counter.
func (m *Metrics) IncPartitionRecordMergeCount() {
	m.partitionRecordMergeCount.Incr(1, m.streamID)
}

// UpdatePartitionCreatedToScheduled records the time taken for a partition to transition from created to scheduled state.
func (m *Metrics) UpdatePartitionCreatedToScheduled(d time.Duration) {
	m.partitionCreatedToScheduled.Timing(d.Nanoseconds(), m.streamID)
}

// UpdatePartitionScheduledToRunning records the time taken for a partition to transition from scheduled to running state.
func (m *Metrics) UpdatePartitionScheduledToRunning(d time.Duration) {
	m.partitionScheduledToRunning.Timing(d.Nanoseconds(), m.streamID)
}

// IncQueryCount increments the query counter.
func (m *Metrics) IncQueryCount() {
	m.queryCount.Incr(1, m.streamID)
}

// IncDataChangeRecordCount increments the data change record counter.
func (m *Metrics) IncDataChangeRecordCount() {
	m.dataChangeRecordCount.Incr(1, m.streamID)
}

// UpdateDataChangeRecordCommittedToEmitted records the latency of a data change
// record in the appropriate bucket.
func (m *Metrics) UpdateDataChangeRecordCommittedToEmitted(d time.Duration) {
	m.dataChangeRecordCommittedToEmitted.Timing(d.Nanoseconds(), m.streamID)
}

// IncHeartbeatRecordCount increments the heartbeat record counter.
func (m *Metrics) IncHeartbeatRecordCount() {
	m.heartbeatRecordCount.Incr(1, m.streamID)
}
