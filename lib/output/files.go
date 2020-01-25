package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: NewFiles,
		Summary: `
Writes each individual message to a new file.`,
		Description: `
In order for each message to create a new file the path must use function
interpolations as described [here](/docs/configuration/interpolation#functions).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("path", "The file to write to, if the file does not yet exist it will be created.").SupportsInterpolation(false),
		},
	}
}

//------------------------------------------------------------------------------

// NewFiles creates a new Files output type.
func NewFiles(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return NewWriter(
		TypeFiles, writer.NewFiles(conf.Files, log, stats), log, stats,
	)
}

//------------------------------------------------------------------------------
