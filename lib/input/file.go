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
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		Description: `
The file type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.

If the delimiter field is left empty then line feed (\n) is used.`,
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration values for the File input type.
type FileConfig struct {
	Path      string `json:"path" yaml:"path"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:      "",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File input type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	file, err := os.Open(conf.File.Path)
	if err != nil {
		return nil, err
	}
	delim := conf.File.Delim
	if len(delim) == 0 {
		delim = "\n"
	}

	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			// Swap so this only works once since we don't want to read the file
			// multiple times.
			if file == nil {
				return nil, io.EOF
			}
			sendFile := file
			file = nil
			return sendFile, nil
		},
		func() {},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.File.MaxBuffer),
		reader.OptLinesSetMultipart(conf.File.Multipart),
	)
	if err != nil {
		return nil, err
	}

	return NewAsyncReader(TypeFile, true, reader.NewAsyncPreserver(rdr), log, stats)
}

//------------------------------------------------------------------------------
