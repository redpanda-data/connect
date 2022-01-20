package processor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"

	// nolint:staticcheck // Ignore SA1019 deprecation warning until we can switch to "google.golang.org/protobuf/types/dynamicpb"
	"github.com/golang/protobuf/proto"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeProtobuf] = TypeSpec{
		constructor: NewProtobuf,
		Categories: []Category{
			CategoryParsing,
		},
		Summary: `
Performs conversions to or from a protobuf message. This processor uses
reflection, meaning conversions can be made directly from the target .proto
files.`,
		Status: docs.StatusBeta,
		Description: `
The main functionality of this processor is to map to and from JSON documents,
you can read more about JSON mapping of protobuf messages here:
[https://developers.google.com/protocol-buffers/docs/proto3#json](https://developers.google.com/protocol-buffers/docs/proto3#json)

Using reflection for processing protobuf messages in this way is less performant
than generating and using native code. Therefore when performance is critical it
is recommended that you use Benthos plugins instead for processing protobuf
messages natively, you can find an example of Benthos plugins at
[https://github.com/benthosdev/benthos-plugin-example](https://github.com/benthosdev/benthos-plugin-example)

## Operators

### ` + "`to_json`" + `

Converts protobuf messages into a generic JSON structure. This makes it easier
to manipulate the contents of the document within Benthos.

### ` + "`from_json`" + `

Attempts to create a target protobuf message from a generic JSON structure.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The [operator](#operators) to execute").HasOptions("to_json", "from_json"),
			docs.FieldCommon("message", "The fully qualified name of the protobuf message to convert to/from."),
			docs.FieldString("import_paths", "A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported.").Array(),
			PartsFieldSpec,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title: "JSON to Protobuf",
				Summary: `
If we have the following protobuf definition within a directory called ` + "`testing/schema`" + `:

` + "```protobuf" + `
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
` + "```" + `

And a stream of JSON documents of the form:

` + "```json" + `
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
` + "```" + `

We can convert the documents into protobuf messages with the following config:`,
				Config: `
pipeline:
  processors:
    - protobuf:
        operator: from_json
        message: testing.Person
        import_paths: [ testing/schema ]
`,
			},
			{
				Title: "Protobuf to JSON",
				Summary: `
If we have the following protobuf definition within a directory called ` + "`testing/schema`" + `:

` + "```protobuf" + `
syntax = "proto3";
package testing;

import "google/protobuf/timestamp.proto";

message Person {
  string first_name = 1;
  string last_name = 2;
  string full_name = 3;
  int32 age = 4;
  int32 id = 5; // Unique ID number for this person.
  string email = 6;

  google.protobuf.Timestamp last_updated = 7;
}
` + "```" + `

And a stream of protobuf messages of the type ` + "`Person`" + `, we could convert them into JSON documents of the format:

` + "```json" + `
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
` + "```" + `

With the following config:`,
				Config: `
pipeline:
  processors:
    - protobuf:
        operator: to_json
        message: testing.Person
        import_paths: [ testing/schema ]
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// ProtobufConfig contains configuration fields for the Protobuf processor.
type ProtobufConfig struct {
	Parts       []int    `json:"parts" yaml:"parts"`
	Operator    string   `json:"operator" yaml:"operator"`
	Message     string   `json:"message" yaml:"message"`
	ImportPaths []string `json:"import_paths" yaml:"import_paths"`
}

// NewProtobufConfig returns a ProtobufConfig with default values.
func NewProtobufConfig() ProtobufConfig {
	return ProtobufConfig{
		Parts:       []int{},
		Operator:    "to_json",
		Message:     "",
		ImportPaths: []string{},
	}
}

//------------------------------------------------------------------------------

type protobufOperator func(part types.Part) error

func newProtobufToJSONOperator(message string, importPaths []string) (protobufOperator, error) {
	m, err := loadDescriptor(message, importPaths)
	if err != nil {
		return nil, err
	}
	return func(part types.Part) error {
		msg := dynamic.NewMessage(m)
		if err := proto.Unmarshal(part.Get(), msg); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		data, err := msg.MarshalJSON()
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message: %w", err)
		}

		part.Set(data)
		return nil
	}, nil
}

func newProtobufFromJSONOperator(message string, importPaths []string) (protobufOperator, error) {
	m, err := loadDescriptor(message, importPaths)
	if err != nil {
		return nil, err
	}
	return func(part types.Part) error {
		msg := dynamic.NewMessage(m)
		if err := msg.UnmarshalJSON(part.Get()); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message: %w", err)
		}

		data, err := msg.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message: %v", err)
		}

		part.Set(data)
		return nil
	}, nil
}

func strToProtobufOperator(opStr, message string, importPaths []string) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(message, importPaths)
	case "from_json":
		return newProtobufFromJSONOperator(message, importPaths)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadDescriptor(message string, importPaths []string) (*desc.MessageDescriptor, error) {
	if message == "" {
		return nil, errors.New("message field must not be empty")
	}

	var parser protoparse.Parser
	if len(importPaths) == 0 {
		importPaths = []string{"."}
	} else {
		parser.ImportPaths = importPaths
	}

	var files []string
	for _, importPath := range importPaths {
		if err := filepath.Walk(importPath, func(path string, info os.FileInfo, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				files = append(files, rPath)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	fds, err := parser.ParseFiles(files...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse .proto file: %v", err)
	}
	if len(fds) == 0 {
		return nil, fmt.Errorf("no .proto files were found in the paths '%v'", importPaths)
	}

	var msg *desc.MessageDescriptor
	for _, d := range fds {
		if msg = d.FindMessage(message); msg != nil {
			break
		}
	}
	if msg == nil {
		err = fmt.Errorf("unable to find message '%v' definition within '%v'", message, importPaths)
	}
	return msg, err
}

//------------------------------------------------------------------------------

// Protobuf is a processor that performs an operation on an Protobuf payload.
type Protobuf struct {
	parts    []int
	operator protobufOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewProtobuf returns an Protobuf processor.
func NewProtobuf(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	p := &Protobuf{
		parts: conf.Protobuf.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	importPaths := conf.Protobuf.ImportPaths

	var err error
	if p.operator, err = strToProtobufOperator(conf.Protobuf.Operator, conf.Protobuf.Message, importPaths); err != nil {
		return nil, err
	}
	return p, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Protobuf) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part types.Part) error {
		if err := p.operator(part); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Operator failed: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2(TypeProtobuf, p.parts, newMsg, proc)

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Protobuf) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *Protobuf) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
