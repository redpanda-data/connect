package config

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/log"
)

// Spec returns a docs.FieldSpec for an entire Benthos configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("http", "Configures the service-wide HTTP server.").WithChildren(api.Spec()...),
		docs.FieldCommon("input", "An input to source messages from.").HasType(docs.FieldInput),
		docs.FieldCommon("buffer", "An optional buffer to store messages during transit.").HasType(docs.FieldBuffer),
		docs.FieldCommon("pipeline", "Describes optional processing pipelines used for mutating messages.").WithChildren(
			docs.FieldCommon("threads", "The number of threads to execute processing pipelines across."),
			docs.FieldCommon("processors", "A list of processors to apply to messages.").Array().HasType(docs.FieldProcessor),
		),
		docs.FieldCommon("output", "An output to sink messages to.").HasType(docs.FieldOutput),
		docs.FieldCommon(
			"resources", "A map of components identified by unique names that can be referenced throughout a Benthos config.",
		).WithChildren(
			docs.FieldCommon("inputs", "A map of inputs.").Map().HasType(docs.FieldInput),
			docs.FieldCommon("conditions", "A map of conditions.").Map().HasType(docs.FieldCondition),
			docs.FieldCommon("processors", "A map of processors.").Map().HasType(docs.FieldProcessor),
			docs.FieldCommon("outputs", "A map of outputs.").Map().HasType(docs.FieldOutput),
			docs.FieldCommon("caches", "A map of caches.").Map().HasType(docs.FieldCache),
			docs.FieldCommon("rate_limits", "A map of rate limits.").Map().HasType(docs.FieldRateLimit),
			docs.FieldAdvanced("plugins", "A map of resource plugins.").Map().HasType(docs.FieldObject),
		),
		docs.FieldCommon("logger", "Describes how operational logs should be emitted.").WithChildren(log.Spec()...),
		docs.FieldCommon("metrics", "A mechanism for exporting metrics.").HasType(docs.FieldMetrics),
		docs.FieldCommon("tracer", "A mechanism for exporting traces.").HasType(docs.FieldTracer),
		docs.FieldCommon("shutdown_timeout", "The maximum period of time to wait for a clean shutdown. If this time is exceeded Benthos will forcefully close."),
		docs.FieldCommon("tests", "Optional unit tests for the config, to be run with the `benthos test` subcommand."),
	}
}
