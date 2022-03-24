package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("hosts", "A list of hosts to connect to.", "localhost:9000").Array(),
			docs.FieldString("user", "A user identifier."),
			docs.FieldString("directory", "A directory to store message files within. If the directory does not exist it will be created."),
			docs.FieldString(
				"path", "The path to upload messages as, interpolation functions should be used in order to generate unique file paths.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
			).IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			policy.FieldSpec(),
		),
		Categories: []string{
			"Services",
		},
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new HDFS output type.
func NewHDFS(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
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
