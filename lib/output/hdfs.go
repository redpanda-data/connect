package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHDFS] = TypeSpec{
		constructor: NewHDFS,
		Description: `
Sends message parts as files to a HDFS directory. Each file is written
with the path specified with the 'path' field, in order to have a different path
for each object you should use function interpolations described
[here](../config_interpolation.md#functions). When sending batched messages the
interpolations are performed per message part.`,
		Async: true,
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new HDFS output type.
func NewHDFS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if conf.HDFS.MaxInFlight == 1 {
		return NewWriter(
			TypeHDFS, writer.NewHDFS(conf.HDFS, log, stats), log, stats,
		)
	}
	return NewAsyncWriter(
		TypeHDFS, conf.HDFS.MaxInFlight, writer.NewHDFS(conf.HDFS, log, stats), log, stats,
	)
}

//------------------------------------------------------------------------------
