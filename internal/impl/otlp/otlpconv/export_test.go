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
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

// LogsToRedpanda converts OTLP log export request to individual Redpanda log
// records. Each log record from the batch becomes a self-contained message
// with embedded Resource/Scope.
func LogsToRedpanda(req plogotlp.ExportRequest) []pb.LogRecord {
	n := LogsCount(req)
	result := make([]pb.LogRecord, 0, n)

	LogsToRedpandaFunc(req, func(log *pb.LogRecord) bool {
		result = append(result, *log) //nolint:govet // copylocks: intentional copy for test helper
		return true
	})

	return result
}

// TracesToRedpanda converts OTLP trace export request to individual Redpanda
// span records. Each span from the batch becomes a self-contained message with
// embedded Resource/Scope.
func TracesToRedpanda(req ptraceotlp.ExportRequest) []pb.Span {
	n := SpansCount(req)
	result := make([]pb.Span, 0, n)

	TracesToRedpandaFunc(req, func(span *pb.Span) bool {
		result = append(result, *span) //nolint:govet // copylocks: intentional copy for test helper
		return true
	})

	return result
}

// MetricsToRedpanda converts OTLP metric export request to individual Redpanda
// metric records. Each metric from the batch becomes a self-contained message
// with embedded Resource/Scope.
func MetricsToRedpanda(req pmetricotlp.ExportRequest) []pb.Metric {
	n := MetricsCount(req)
	result := make([]pb.Metric, 0, n)

	MetricsToRedpandaFunc(req, func(metric *pb.Metric) bool {
		result = append(result, *metric) //nolint:govet // copylocks: intentional copy for test helper
		return true
	})

	return result
}
