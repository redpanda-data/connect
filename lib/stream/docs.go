package stream

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("input", "An input to source messages from.").HasType(docs.FieldInput),
		docs.FieldCommon("buffer", "An optional buffer to store messages during transit.").HasType(docs.FieldBuffer),
		docs.FieldCommon("pipeline", "Describes optional processing pipelines used for mutating messages.").WithChildren(
			docs.FieldCommon("threads", "The number of threads to execute processing pipelines across."),
			docs.FieldCommon("processors", "A list of processors to apply to messages.").Array().HasType(docs.FieldProcessor),
		),
		docs.FieldCommon("output", "An output to sink messages to.").HasType(docs.FieldOutput),
	}
}
