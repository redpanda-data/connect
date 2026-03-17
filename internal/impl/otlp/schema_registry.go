// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	rpotel "github.com/redpanda-data/common-go/redpanda-otel-exporter"
	"github.com/redpanda-data/connect/v4/internal/schemaregistry"
)

const (
	schemaRegistryField = "schema_registry"

	srFieldCommonSubject = "common_subject"
	srFieldTraceSubject  = "trace_subject"
	srFieldLogSubject    = "log_subject"
	srFieldMetricSubject = "metric_subject"
)

// schemaRegistryConfigFields returns the configuration fields for Schema Registry integration.
// This includes both the standard SR client fields (url, timeout, tls, auth) and
// custom subject name fields for OTLP schemas.
func schemaRegistryConfigFields() []*service.ConfigField {
	fields := schemaregistry.ConfigFields()

	// Add subject configuration fields with defaults from exporter constants.
	fields = append(fields,
		service.NewStringField(srFieldCommonSubject).
			Description("Schema subject name for the common protobuf schema. Only used when encoding is 'protobuf'. Defaults to 'redpanda-otel-common' for protobuf encoding or 'redpanda-otel-common-json' for JSON encoding.").
			Default("").
			Advanced(),
		service.NewStringField(srFieldTraceSubject).
			Description("Schema subject name for trace data. Defaults to 'redpanda-otel-traces' for protobuf encoding or 'redpanda-otel-traces-json' for JSON encoding.").
			Default("").
			Advanced(),
		service.NewStringField(srFieldLogSubject).
			Description("Schema subject name for log data. Defaults to 'redpanda-otel-logs' for protobuf encoding or 'redpanda-otel-logs-json' for JSON encoding.").
			Default("").
			Advanced(),
		service.NewStringField(srFieldMetricSubject).
			Description("Schema subject name for metric data. Defaults to 'redpanda-otel-metrics' for protobuf encoding or 'redpanda-otel-metrics-json' for JSON encoding.").
			Default("").
			Advanced(),
	)

	return fields
}

// defaultSubject returns the default subject name for a given signal type and
// encoding.
func defaultSubject(signalType SignalType, encoding Encoding) string {
	switch signalType {
	case SignalTypeTrace:
		if encoding == EncodingJSON {
			return rpotel.DefaultTraceSubjectJSON
		}
		return rpotel.DefaultTraceSubject
	case SignalTypeLog:
		if encoding == EncodingJSON {
			return rpotel.DefaultLogSubjectJSON
		}
		return rpotel.DefaultLogSubject
	case SignalTypeMetric:
		if encoding == EncodingJSON {
			return rpotel.DefaultMetricSubjectJSON
		}
		return rpotel.DefaultMetricSubject
	default:
		return ""
	}
}

// defaultCommonSubject returns the default common subject name for the given encoding.
func defaultCommonSubject(encoding Encoding) string {
	if encoding == EncodingJSON {
		return rpotel.DefaultCommonSubjectJSON
	}
	return rpotel.DefaultCommonSubject
}
