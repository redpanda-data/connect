package stream

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// Spec returns a docs.FieldSpec for a stream configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldInput("input", "An input to source messages from.").Optional(),
		docs.FieldBuffer("buffer", "An optional buffer to store messages during transit.").Optional(),
		pipeline.ConfigSpec(),
		docs.FieldOutput("output", "An output to sink messages to.").Optional(),
	}
}
