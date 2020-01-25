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
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		Summary: `
Writes messages in line delimited format to a file.`,
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
			docs.FieldCommon("path", "The file to write to, if the file does not yet exist it will be created."),
			docs.FieldCommon("delimiter", "A custom delimiter to separate messages with. If left empty defaults to a line break."),
		},
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration fields for the file based output type.
type FileConfig struct {
	Path  string `json:"path" yaml:"path"`
	Delim string `json:"delimiter" yaml:"delimiter"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:  "",
		Delim: "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File output type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	file, err := os.OpenFile(conf.File.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0666))
	if err != nil {
		return nil, err
	}
	return NewLineWriter(file, true, []byte(conf.File.Delim), "file", log, stats)
}

//------------------------------------------------------------------------------
