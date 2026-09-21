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

// snowpipePipeMetrics holds the snowflake_streaming_pipe output's metrics.
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

// ReportEmptyRowsSkipped counts messages with an empty body (tombstones)
// that were never sent to Snowflake.
func (m *snowpipePipeMetrics) ReportEmptyRowsSkipped(count int64) {
	m.emptyRowsSkipped.Incr(count)
}

func (m *snowpipePipeMetrics) Report(appendTime, commitTime time.Duration) {
	m.appendTime.Timing(appendTime.Nanoseconds())
	m.commitTime.Timing(commitTime.Nanoseconds())
}

// ReportRowsRejected counts newly rejected rows (a delta, not the channel's
// lifetime total).
func (m *snowpipePipeMetrics) ReportRowsRejected(count int64) {
	m.rowsErrorCount.Incr(count)
}

// ReportOutOfOrderSubmission counts batches refused by checkSubmissionOrder;
// any increment indicates a channel_name/max_in_flight misconfiguration.
func (m *snowpipePipeMetrics) ReportOutOfOrderSubmission() {
	m.outOfOrderSubmissions.Incr(1)
}
