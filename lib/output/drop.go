package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDrop] = TypeSpec{
		constructor: fromSimpleConstructor(NewDrop),
		Summary: `
Drops all messages.`,
		Categories: []Category{
			CategoryUtility,
		},
		config: docs.FieldComponent().HasType(docs.FieldTypeObject),
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
