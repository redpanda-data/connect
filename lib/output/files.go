package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: NewFiles,
		Description: `
Writes each individual part of each message to a new file.

Message parts only contain raw data, and therefore in order to create a unique
file for each part you need to generate unique file names. This can be done by
using function interpolations on the ` + "`path`" + ` field as described
[here](/docs/configuration/interpolation#functions). When sending batched messages
these interpolations are performed per message part.`,
	}
}

//------------------------------------------------------------------------------

// NewFiles creates a new Files output type.
func NewFiles(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return NewWriter(
		TypeFiles, writer.NewFiles(conf.Files, log, stats), log, stats,
	)
}

//------------------------------------------------------------------------------
