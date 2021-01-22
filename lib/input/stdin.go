package input

import (
	"io"
	"os"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSTDIN] = TypeSpec{
		constructor: fromSimpleConstructor(NewSTDIN),
		Summary: `
Consumes data piped to stdin as line delimited messages.`,
		Description: `
If the multipart option is set to true then lines are interpretted as message
parts, and an empty line indicates the end of the message.

If the delimiter field is left empty then line feed (\n) is used.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced("multipart", "Whether messages should be consumed as multiple parts. If so, each line is consumed as a message parts and the full message ends with an empty line."),
			docs.FieldAdvanced("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed."),
			docs.FieldAdvanced("delimiter", "The delimiter to use to detect the end of each message. If left empty line breaks are used."),
		},
		Categories: []Category{
			CategoryLocal,
		},
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
