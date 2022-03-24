package input

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHDFS] = TypeSpec{
		constructor: fromSimpleConstructor(NewHDFS),
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
		Categories: []string{
			"Services",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("hosts", "A list of target host addresses to connect to.").Array(),
			docs.FieldString("user", "A user ID to connect as."),
			docs.FieldString("directory", "The directory to consume from."),
		),
	}
}

//------------------------------------------------------------------------------

// NewHDFS creates a new Files input type.
func NewHDFS(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	if conf.HDFS.Directory == "" {
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
