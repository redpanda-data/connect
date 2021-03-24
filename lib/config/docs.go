package config

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
)

// Spec returns a docs.FieldSpec for an entire Benthos configuration.
func Spec() docs.FieldSpecs {
	fields := docs.FieldSpecs{
		docs.FieldCommon("http", "Configures the service-wide HTTP server.").WithChildren(api.Spec()...),
		docs.FieldCommon("input", "An input to source messages from.").HasType(docs.FieldInput),
		docs.FieldCommon("buffer", "An optional buffer to store messages during transit.").HasType(docs.FieldBuffer),
		docs.FieldCommon("pipeline", "Describes optional processing pipelines used for mutating messages.").WithChildren(
			docs.FieldCommon("threads", "The number of threads to execute processing pipelines across."),
			docs.FieldCommon("processors", "A list of processors to apply to messages.").Array().HasType(docs.FieldProcessor),
		),
		docs.FieldCommon("output", "An output to sink messages to.").HasType(docs.FieldOutput),
	}

	fields = append(fields, manager.Spec()...)

	fields = append(fields, docs.FieldSpecs{
		docs.FieldCommon("logger", "Describes how operational logs should be emitted.").WithChildren(log.Spec()...),
		docs.FieldCommon("metrics", "A mechanism for exporting metrics.").HasType(docs.FieldMetrics),
		docs.FieldCommon("tracer", "A mechanism for exporting traces.").HasType(docs.FieldTracer),
		docs.FieldCommon("shutdown_timeout", "The maximum period of time to wait for a clean shutdown. If this time is exceeded Benthos will forcefully close."),
		docs.FieldCommon("tests", "Optional unit tests for the config, to be run with the `benthos test` subcommand."),
	}...)

	return fields
}
