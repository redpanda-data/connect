package stream

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldInput("input", "An input to source messages from.").Optional(),
		docs.FieldBuffer("buffer", "An optional buffer to store messages during transit.").Optional(),
		docs.FieldObject("pipeline", "Describes optional processing pipelines used for mutating messages.").WithChildren(
			docs.FieldInt("threads", "The number of threads to execute processing pipelines across.").HasDefault(-1),
			docs.FieldProcessor("processors", "A list of processors to apply to messages.").Array().HasDefault([]any{}),
		),
		docs.FieldOutput("output", "An output to sink messages to.").Optional(),
	}
}
