package stream

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldInput(fieldInput, "An input to source messages from.").HasDefault(map[string]any{
			"stdin": map[string]any{},
		}),
		docs.FieldBuffer(fieldBuffer, "An optional buffer to store messages during transit.").HasDefault(map[string]any{
			"none": map[string]any{},
		}),
		pipeline.ConfigSpec(),
		docs.FieldOutput(fieldOutput, "An output to sink messages to.").HasDefault(map[string]any{
			"stdout": map[string]any{},
		}),
	}
}
