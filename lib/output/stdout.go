package output

import (
	"os"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSTDOUT] = TypeSpec{
		constructor: NewSTDOUT,
		Description: `
The stdout output type prints messages to stdout. Single part messages are
printed with a delimiter (defaults to '\n' if left empty). Multipart messages
are written with each part delimited, with the final part followed by two
delimiters, e.g. a multipart message [ "foo", "bar", "baz" ] would be written
as:

foo\n
bar\n
baz\n\n`,
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
