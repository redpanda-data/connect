package input

import (
	"io"
	"os"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSTDIN] = TypeSpec{
		constructor: NewSTDIN,
		Description: `
The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

If the delimiter field is left empty then line feed (\n) is used.`,
	}
}

//------------------------------------------------------------------------------

// STDINConfig contains config fields for the STDIN input type.
type STDINConfig struct {
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewSTDINConfig creates a STDINConfig populated with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// NewSTDIN creates a new STDIN input type.
func NewSTDIN(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	delim := conf.STDIN.Delim
	if len(delim) == 0 {
		delim = "\n"
	}

	stdin := os.Stdin
	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			// Swap so this only works once since we don't want to read stdin
			// multiple times.
			if stdin == nil {
				return nil, io.EOF
			}
			sendStdin := stdin
			stdin = nil
			return sendStdin, nil
		},
		func() {},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.STDIN.MaxBuffer),
		reader.OptLinesSetMultipart(conf.STDIN.Multipart),
	)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeSTDIN, true,
		reader.NewAsyncCutOff(reader.NewAsyncPreserver(rdr)),
		log, stats,
	)
}

//------------------------------------------------------------------------------
