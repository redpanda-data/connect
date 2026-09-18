// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// snowpipePipeMetrics holds the metrics reported by the snowflake_streaming_pipe
// output. Unlike snowpipeMetrics (the v1 snowflake_streaming output's
// metrics), there is no Parquet-build-and-stage-upload pipeline to measure
// here -- SSv2 sends NDJSON directly over HTTP -- so this type tracks only
// the two calls this output actually makes (AppendRows, and the poll behind
// WaitUntilCommitted) plus the rows Snowflake rejects asynchronously after
// appending.
type snowpipePipeMetrics struct {
	appendTime            *service.MetricTimer
	commitTime            *service.MetricTimer
	rowsErrorCount        *service.MetricCounter
	outOfOrderSubmissions *service.MetricCounter
	emptyRowsSkipped      *service.MetricCounter
}

func newSnowpipePipeMetrics(m *service.Metrics) *snowpipePipeMetrics {
	return &snowpipePipeMetrics{
		appendTime:            m.NewTimer("snowflake_append_latency_ns"),
		commitTime:            m.NewTimer("snowflake_commit_latency_ns"),
		rowsErrorCount:        m.NewCounter("snowflake_rows_error_count"),
		outOfOrderSubmissions: m.NewCounter("snowflake_out_of_order_submissions"),
		emptyRowsSkipped:      m.NewCounter("snowflake_empty_rows_skipped"),
	}
}

// ReportEmptyRowsSkipped increments snowflake_empty_rows_skipped by the
// number of messages in a batch whose body was empty or whitespace-only and
// so was never sent to Snowflake (there is no row representation for them;
// Kafka/Redpanda tombstones are the usual source). These are not errors --
// dropping them matches the Kafka Connector's default -- but without a
// metric the drop would be invisible outside debug logs.
func (m *snowpipePipeMetrics) ReportEmptyRowsSkipped(count int64) {
	m.emptyRowsSkipped.Incr(count)
}

func (m *snowpipePipeMetrics) Report(appendTime, commitTime time.Duration) {
	m.appendTime.Timing(appendTime.Nanoseconds())
	m.commitTime.Timing(commitTime.Nanoseconds())
}

// ReportRowsRejected increments snowflake_rows_error_count by the number of
// rows Snowflake counted as rejected since the last time this channel
// reported a positive delta (see streamingv2.IngestChannel.RowsRejected) --
// a delta, not the channel's cumulative lifetime total.
func (m *snowpipePipeMetrics) ReportRowsRejected(count int64) {
	m.rowsErrorCount.Incr(count)
}

// ReportOutOfOrderSubmission increments snowflake_out_of_order_submissions
// each time checkSubmissionOrder rejects a batch. Unlike RowsRejected --
// which a healthy pipeline can legitimately report from time to time -- a
// single increment here means offset_token's ordering invariant was
// violated and, per checkSubmissionOrder's own doc comment, will keep being
// violated on retry until the underlying channel_name/max_in_flight
// misconfiguration is fixed. Without a dedicated metric this was only
// visible in logs.
func (m *snowpipePipeMetrics) ReportOutOfOrderSubmission() {
	m.outOfOrderSubmissions.Incr(1)
}
