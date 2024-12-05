/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package snowflake

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

type snowpipeMetrics struct {
	compressedOutput *service.MetricCounter
	uploadTime       *service.MetricTimer
	buildTime        *service.MetricTimer
	convertTime      *service.MetricTimer
	serializeTime    *service.MetricTimer
}

func newSnowpipeMetrics(m *service.Metrics) *snowpipeMetrics {
	return &snowpipeMetrics{
		buildTime:        m.NewTimer("snowflake_build_output_latency_ns"),
		uploadTime:       m.NewTimer("snowflake_upload_latency_ns"),
		convertTime:      m.NewTimer("snowflake_convert_latency_ns"),
		serializeTime:    m.NewTimer("snowflake_serialize_latency_ns"),
		compressedOutput: m.NewCounter("snowflake_compressed_output_size_bytes"),
	}
}

func (m *snowpipeMetrics) Report(stats streaming.InsertStats) {
	m.compressedOutput.Incr(int64(stats.CompressedOutputSize))
	m.uploadTime.Timing(stats.UploadTime.Nanoseconds())
	m.buildTime.Timing(stats.BuildTime.Nanoseconds())
	m.convertTime.Timing(stats.ConvertTime.Nanoseconds())
	m.serializeTime.Timing(stats.SerializeTime.Nanoseconds())
}
