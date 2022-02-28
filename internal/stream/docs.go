package stream

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("input", "An input to source messages from.").HasType(docs.FieldTypeInput),
		docs.FieldCommon("buffer", "An optional buffer to store messages during transit.").HasType(docs.FieldTypeBuffer),
		docs.FieldCommon("pipeline", "Describes optional processing pipelines used for mutating messages.").WithChildren(
			docs.FieldInt("threads", "The number of threads to execute processing pipelines across.").HasDefault(1),
			docs.FieldCommon("processors", "A list of processors to apply to messages.").Array().HasType(docs.FieldTypeProcessor),
		),
		docs.FieldCommon("output", "An output to sink messages to.").HasType(docs.FieldTypeOutput),
	}
}
