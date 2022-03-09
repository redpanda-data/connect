package config

import (
	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

// Type is the Benthos service configuration struct.
type Type struct {
	HTTP                   api.Config `json:"http" yaml:"http"`
	stream.Config          `json:",inline" yaml:",inline"`
	manager.ResourceConfig `json:",inline" yaml:",inline"`
	Logger                 log.Config     `json:"logger" yaml:"logger"`
	Metrics                metrics.Config `json:"metrics" yaml:"metrics"`
	Tracer                 tracer.Config  `json:"tracer" yaml:"tracer"`
	SystemCloseTimeout     string         `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	Tests                  []interface{}  `json:"tests,omitempty" yaml:"tests,omitempty"`
}

// New returns a new configuration with default values.
func New() Type {
	return Type{
		HTTP:               api.NewConfig(),
		Config:             stream.NewConfig(),
		ResourceConfig:     manager.NewResourceConfig(),
		Logger:             log.NewConfig(),
		Metrics:            metrics.NewConfig(),
		Tracer:             tracer.NewConfig(),
		SystemCloseTimeout: "20s",
		Tests:              nil,
	}
}

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
