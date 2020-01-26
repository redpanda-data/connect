package output

import (
	"os"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSTDOUT] = TypeSpec{
		constructor: NewSTDOUT,
		Summary: `
The stdout output type prints messages to stdout.`,
		Description: `
Each message written is followed by a delimiter (defaults to '\n' if left empty)
and when sending multipart messages (message batches) the last message ends with
double delimiters. E.g. the messages "foo", "bar" and "baz" would be written as:

` + "```" + `
foo\n
bar\n
baz\n
` + "```" + `

Whereas a multipart message [ "foo", "bar", "baz" ] would be written as:

` + "```" + `
foo\n
bar\n
baz\n\n
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("delimiter", "A custom delimiter to separate messages with. If left empty defaults to a line break."),
		},
	}
}

//------------------------------------------------------------------------------

// STDOUTConfig contains configuration fields for the stdout based output type.
type STDOUTConfig struct {
	Delim string `json:"delimiter" yaml:"delimiter"`
}

// NewSTDOUTConfig creates a new STDOUTConfig with default values.
func NewSTDOUTConfig() STDOUTConfig {
	return STDOUTConfig{
		Delim: "",
	}
}

//------------------------------------------------------------------------------

// NewSTDOUT creates a new STDOUT output type.
func NewSTDOUT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return NewLineWriter(os.Stdout, false, []byte(conf.STDOUT.Delim), "stdout", log, stats)
}

//------------------------------------------------------------------------------
