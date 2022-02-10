package output

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHDFS] = TypeSpec{
		constructor: fromSimpleConstructor(NewHDFS),
		Summary: `
Sends message parts as files to a HDFS directory.`,
		Description: `
Each file is written with the path specified with the 'path' field, in order to
have a different path for each object you should use function interpolations
described [here](/docs/configuration/interpolation#bloblang-queries).`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("hosts", "A list of hosts to connect to.", "localhost:9000").Array(),
			docs.FieldCommon("user", "A user identifier."),
			docs.FieldCommon("directory", "A directory to store message files within. If the directory does not exist it will be created."),
			docs.FieldCommon(
				"path", "The path to upload messages as, interpolation functions should be used in order to generate unique file paths.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
			).IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new HDFS output type.
func NewHDFS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	h, err := writer.NewHDFSV2(conf.HDFS, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeHDFS, conf.HDFS.MaxInFlight, h, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.HDFS.Batching, OnlySinglePayloads(w), mgr, log, stats)
}

//------------------------------------------------------------------------------
