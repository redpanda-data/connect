package config

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/stream"
)

var httpField = docs.FieldCommon("http", "Configures the service-wide HTTP server.").WithChildren(api.Spec()...)

var observabilityFields = docs.FieldSpecs{
	docs.FieldCommon("logger", "Describes how operational logs should be emitted.").WithChildren(log.Spec()...),
	docs.FieldCommon("metrics", "A mechanism for exporting metrics.").HasType(docs.FieldTypeMetrics),
	docs.FieldCommon("tracer", "A mechanism for exporting traces.").HasType(docs.FieldTypeTracer),
	docs.FieldString("shutdown_timeout", "The maximum period of time to wait for a clean shutdown. If this time is exceeded Benthos will forcefully close.").HasDefault("20s"),
}

// TestsField describes the optional test definitions field at the root of a
// benthos config.
var TestsField = docs.FieldCommon("tests", "Optional unit tests for the config, to be run with the `benthos test` subcommand.").Array().HasType(docs.FieldTypeUnknown).HasDefault([]interface{}{})

// Spec returns a docs.FieldSpec for an entire Benthos configuration.
func Spec() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, stream.Spec()...)
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields...)
	fields = append(fields, TestsField)
	return fields
}

// SpecWithoutStream describes a stream config without the core stream fields.
func SpecWithoutStream() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields...)
	fields = append(fields, TestsField)
	return fields
}
