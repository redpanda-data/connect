package input

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHDFS] = TypeSpec{
		constructor: NewHDFS,
		Description: `
Reads files from a HDFS directory, where each discrete file will be consumed as
a single message payload.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- hdfs_name
- hdfs_path
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
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
