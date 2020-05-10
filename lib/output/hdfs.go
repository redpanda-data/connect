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
	Constructors[TypeHDFS] = TypeSpec{
		constructor: NewHDFS,
		Summary: `
Sends message parts as files to a HDFS directory.`,
		Description: `
Each file is written with the path specified with the 'path' field, in order to
have a different path for each object you should use function interpolations
described [here](/docs/configuration/interpolation#functions). When sending
batched messages the interpolations are performed per message part.`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("hosts", "A list of hosts to connect to.", "localhost:9000"),
			docs.FieldCommon("user", "A user identifier."),
			docs.FieldCommon("directory", "A directory to store message files within. If the directory does not exist it will be created."),
			docs.FieldCommon(
				"path", "The path to upload messages as, interpolation functions should be used in order to generate unique file paths.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
			).SupportsInterpolation(false),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new HDFS output type.
func NewHDFS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	h, err := writer.NewHDFS(conf.HDFS, log, stats)
	if err != nil {
		return nil, err
	}
	if conf.HDFS.MaxInFlight == 1 {
		return NewWriter(
			TypeHDFS, h, log, stats,
		)
	}
	return NewAsyncWriter(
		TypeHDFS, conf.HDFS.MaxInFlight, h, log, stats,
	)
}

//------------------------------------------------------------------------------
