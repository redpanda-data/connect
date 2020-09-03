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
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		Summary: `
Reads a file, where each line is processed as an individual message.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("path", "A path pointing to a file on disk."),
			docs.FieldCommon("multipart", `
If set `+"`true`"+` each line is read as a message part, and an empty line
indicates the end of a message batch, and only then is the batch flushed
downstream.`),
			docs.FieldCommon("max_buffer", "Must be larger than the largest line of the target file."),
			docs.FieldCommon("delimiter", `
A string that indicates the end of a message within the target file. If left
empty then line feed (\n) is used.`),
		},
		Categories: []Category{
			CategoryLocal,
		},
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
