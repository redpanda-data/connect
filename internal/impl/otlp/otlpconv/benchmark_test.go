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

package otlpconv

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// createBenchmarkTraces creates a batch of traces with the specified number of spans.
func createBenchmarkTraces(numSpans int) ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()

	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "benchmark-service")
	rs.Resource().Attributes().PutStr("host.name", "benchmark-host")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("benchmark-instrumentation")
	ss.Scope().SetVersion("1.0.0")

	traceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

	for i := range numSpans {
		span := ss.Spans().AppendEmpty()
		spanID := [8]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), 0x00, 0x00, 0x00, 0x00}

		span.SetTraceID(traceID)
		span.SetSpanID(spanID)
		span.SetName("benchmark-span")
		span.SetKind(ptrace.SpanKindServer)
		span.SetStartTimestamp(1000000000)
		span.SetEndTimestamp(2000000000)

		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutStr("http.url", "/api/benchmark")
		span.Attributes().PutInt("http.status_code", 200)

		event := span.Events().AppendEmpty()
		event.SetName("benchmark-event")
		event.SetTimestamp(1500000000)
	}

	return ptraceotlp.NewExportRequestFromTraces(traces)
}

// createBenchmarkLogs creates a batch of logs with the specified number of log records.
func createBenchmarkLogs(numLogs int) plogotlp.ExportRequest {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "benchmark-service")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("benchmark-logger")

	for i := range numLogs {
		log := sl.LogRecords().AppendEmpty()
		log.SetTimestamp(pcommon.Timestamp(1000000000 + int64(i)))
		log.SetSeverityNumber(plog.SeverityNumberInfo)
		log.SetSeverityText("INFO")
		log.Body().SetStr("This is a benchmark log message")
		log.Attributes().PutStr("log.id", "benchmark-log")
		log.Attributes().PutInt("log.index", int64(i))
	}

	return plogotlp.NewExportRequestFromLogs(logs)
}

// createBenchmarkMetrics creates a batch of metrics with the specified number of metrics.
func createBenchmarkMetrics(numMetrics int) pmetricotlp.ExportRequest {
	metrics := pmetric.NewMetrics()

	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "benchmark-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("benchmark-meter")

	for i := range numMetrics {
		metric := sm.Metrics().AppendEmpty()
		metric.SetName("benchmark.gauge")
		metric.SetDescription("Benchmark gauge metric")
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1000000000))
		dp.SetDoubleValue(float64(i))
		dp.Attributes().PutStr("metric.id", "benchmark-metric")
	}

	return pmetricotlp.NewExportRequestFromMetrics(metrics)
}

// BenchmarkTracesToRedpanda benchmarks OTLP to Redpanda trace conversion.
func BenchmarkTracesToRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkTraces(size)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				TracesToRedpanda(req)
			}

			// Report spans per second
			spansPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(spansPerSec, "spans/sec")
		})
	}
}

// BenchmarkTracesFromRedpanda benchmarks Redpanda to OTLP trace conversion.
func BenchmarkTracesFromRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkTraces(size)
			redpandaSpans := TracesToRedpanda(req)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				TracesFromRedpanda(redpandaSpans)
			}

			// Report spans per second
			spansPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(spansPerSec, "spans/sec")
		})
	}
}

// BenchmarkLogsToRedpanda benchmarks OTLP to Redpanda log conversion.
func BenchmarkLogsToRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkLogs(size)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				LogsToRedpanda(req)
			}

			// Report logs per second
			logsPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(logsPerSec, "logs/sec")
		})
	}
}

// BenchmarkLogsFromRedpanda benchmarks Redpanda to OTLP log conversion.
func BenchmarkLogsFromRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkLogs(size)
			redpandaLogs := LogsToRedpanda(req)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				LogsFromRedpanda(redpandaLogs)
			}

			// Report logs per second
			logsPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(logsPerSec, "logs/sec")
		})
	}
}

// BenchmarkMetricsToRedpanda benchmarks OTLP to Redpanda metric conversion.
func BenchmarkMetricsToRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkMetrics(size)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				MetricsToRedpanda(req)
			}

			// Report metrics per second
			metricsPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(metricsPerSec, "metrics/sec")
		})
	}
}

// BenchmarkMetricsFromRedpanda benchmarks Redpanda to OTLP metric conversion.
func BenchmarkMetricsFromRedpanda(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(formatBenchmarkName(size), func(b *testing.B) {
			req := createBenchmarkMetrics(size)
			redpandaMetrics := MetricsToRedpanda(req)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				MetricsFromRedpanda(redpandaMetrics)
			}

			// Report metrics per second
			metricsPerSec := float64(size*b.N) / b.Elapsed().Seconds()
			b.ReportMetric(metricsPerSec, "metrics/sec")
		})
	}
}

// formatBenchmarkName creates a human-readable benchmark name from size.
func formatBenchmarkName(size int) string {
	if size >= 1000 {
		return fmt.Sprintf("%dk", size/1000)
	}
	return fmt.Sprintf("%d", size)
}
