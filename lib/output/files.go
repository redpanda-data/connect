package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: fromSimpleConstructor(NewFiles),
		Status:      docs.StatusDeprecated,
		Summary: `
Writes each individual message to a new file.`,
		Description: `
## Alternatives

The functionality of this output is now supported by the ` + "[`file`](/docs/components/outputs/file)" + ` output.

In order for each message to create a new file the path must use function
interpolations as described [here](/docs/configuration/interpolation#bloblang-queries).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("path", "The file to write to, if the file does not yet exist it will be created.").IsInterpolated(),
		},
		Categories: []Category{
			CategoryLocal,
		},
	}
}

//------------------------------------------------------------------------------

// NewFiles creates a new Files output type.
func NewFiles(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	f, err := writer.NewFilesV2(conf.Files, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		TypeFiles, f, log, stats,
	)
}

//------------------------------------------------------------------------------
