package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: fromSimpleConstructor(NewFiles),
		Status:      docs.StatusDeprecated,
		Summary: `
Reads files from a path, where each discrete file will be consumed as a single
message.`,
		Description: `
The path can either point to a single file (resulting in only a single message)
or a directory, in which case the directory will be walked and each file found
will become a message.

## Alternatives

The behaviour of this input is now covered by the ` + "[`file` input](/docs/components/inputs/file)" + `.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- path
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []Category{
			CategoryLocal,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("path", "A path to either a directory or a file."),
			docs.FieldCommon("delete_files", "Whether to delete files once they are consumed."),
		},
	}
}

//------------------------------------------------------------------------------

// NewFiles creates a new Files input type.
func NewFiles(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var f reader.Async
	var err error
	if f, err = reader.NewFiles(conf.Files); err != nil {
		return nil, err
	}
	f = reader.NewAsyncPreserver(f)
	return NewAsyncReader(TypeFiles, true, f, log, stats, nil)
}

//------------------------------------------------------------------------------
