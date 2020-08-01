package input

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHDFS] = TypeSpec{
		constructor: NewHDFS,
		Summary: `
Reads files from a HDFS directory, where each discrete file will be consumed as
a single message payload.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- hdfs_name
- hdfs_path
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("hosts", "A list of target host addresses to connect to."),
			docs.FieldCommon("user", "A user ID to connect as."),
			docs.FieldCommon("directory", "The directory to consume from."),
		},
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new Files input type.
func NewHDFS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.HDFS.Directory) == 0 {
		return nil, errors.New("invalid directory (cannot be empty)")
	}
	return NewAsyncReader(
		TypeHDFS,
		true,
		reader.NewAsyncPreserver(
			reader.NewHDFS(conf.HDFS, log, stats),
		),
		log, stats,
	)
}

//------------------------------------------------------------------------------
