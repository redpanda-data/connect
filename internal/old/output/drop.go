package output

import (
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDrop] = TypeSpec{
		constructor: fromSimpleConstructor(NewDrop),
		Summary: `
Drops all messages.`,
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldObject("", ""),
	}
}

//------------------------------------------------------------------------------

// NewDrop creates a new Drop output type.
func NewDrop(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	return NewAsyncWriter(
		TypeDrop, 1, writer.NewDrop(conf.Drop, log, stats), log, stats,
	)
}

//------------------------------------------------------------------------------
