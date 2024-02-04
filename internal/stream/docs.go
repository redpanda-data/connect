package stream

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	defaultInput := map[string]any{"inproc": ""}
	if _, exists := bundle.GlobalEnvironment.GetDocs("stdin", docs.TypeInput); exists {
		defaultInput = map[string]any{
			"stdin": map[string]any{},
		}
	}

	defaultOutput := map[string]any{"inproc": ""}
	if _, exists := bundle.GlobalEnvironment.GetDocs("stdout", docs.TypeOutput); exists {
		defaultOutput = map[string]any{
			"stdout": map[string]any{},
		}
	}

	return docs.FieldSpecs{
		docs.FieldInput(fieldInput, "An input to source messages from.").HasDefault(defaultInput),
		docs.FieldBuffer(fieldBuffer, "An optional buffer to store messages during transit.").HasDefault(map[string]any{
			"none": map[string]any{},
		}),
		pipeline.ConfigSpec(),
		docs.FieldOutput(fieldOutput, "An output to sink messages to.").HasDefault(defaultOutput),
	}
}
