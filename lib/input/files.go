package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: NewFiles,
		Description: `
Reads files from a path, where each discrete file will be consumed as a single
message payload. The path can either point to a single file (resulting in only a
single message) or a directory, in which case the directory will be walked and
each file found will become a message.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- path
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
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
	return NewAsyncReader(TypeFiles, true, f, log, stats)
}

//------------------------------------------------------------------------------
