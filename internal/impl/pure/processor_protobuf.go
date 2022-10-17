package pure

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"

	// nolint:staticcheck // Ignore SA1019 deprecation warning until we can switch to "google.golang.org/protobuf/types/dynamicpb"
	"github.com/golang/protobuf/jsonpb"
	// nolint:staticcheck // Ignore SA1019 deprecation warning until we can switch to "google.golang.org/protobuf/types/dynamicpb"
	"github.com/golang/protobuf/proto"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newProtobuf(conf.Protobuf, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("protobuf", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "protobuf",
		Categories: []string{
			"Parsing",
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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("operator", "The [operator](#operators) to execute").HasOptions("to_json", "from_json"),
			docs.FieldString("message", "The fully qualified name of the protobuf message to convert to/from."),
			docs.FieldString("import_paths", "A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported.").Array(),
		).ChildDefaultAndTypesFromStruct(processor.NewProtobufConfig()),
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
	})
	if err != nil {
		panic(err)
	}
}

type protobufOperator func(part *message.Part) error

func newProtobufToJSONOperator(f ifs.FS, msg string, importPaths []string) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	descriptors, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	m := getMessageFromDescriptors(msg, descriptors)
	if m == nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	marshaller := &jsonpb.Marshaler{
		AnyResolver: dynamic.AnyResolver(dynamic.NewMessageFactoryWithDefaults(), descriptors...),
	}

	return func(part *message.Part) error {
		msg := dynamic.NewMessage(m)
		if err := proto.Unmarshal(part.AsBytes(), msg); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		data, err := msg.MarshalJSONPB(marshaller)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message: %w", err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONOperator(f ifs.FS, msg string, importPaths []string) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	descriptors, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	m := getMessageFromDescriptors(msg, descriptors)
	if m == nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	unmarshaler := &jsonpb.Unmarshaler{
		AnyResolver: dynamic.AnyResolver(dynamic.NewMessageFactoryWithDefaults(), descriptors...),
	}

	return func(part *message.Part) error {
		msg := dynamic.NewMessage(m)
		if err := msg.UnmarshalJSONPB(unmarshaler, part.AsBytes()); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message: %w", err)
		}

		data, err := msg.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message: %v", err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func strToProtobufOperator(f ifs.FS, opStr, message string, importPaths []string) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(f, message, importPaths)
	case "from_json":
		return newProtobufFromJSONOperator(f, message, importPaths)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadDescriptors(f ifs.FS, importPaths []string) ([]*desc.FileDescriptor, error) {
	var parser protoparse.Parser
	if len(importPaths) == 0 {
		importPaths = []string{"."}
	} else {
		parser.ImportPaths = importPaths
	}

	var files []string
	for _, importPath := range importPaths {
		if err := fs.WalkDir(f, importPath, func(path string, info fs.DirEntry, ferr error) error {
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

	return fds, err
}

func getMessageFromDescriptors(message string, fds []*desc.FileDescriptor) *desc.MessageDescriptor {
	var msg *desc.MessageDescriptor
	for _, fd := range fds {
		msg = fd.FindMessage(message)
		if msg != nil {
			break
		}
	}
	return msg
}

//------------------------------------------------------------------------------

type protobufProc struct {
	operator protobufOperator
	log      log.Modular
}

func newProtobuf(conf processor.ProtobufConfig, mgr bundle.NewManagement) (*protobufProc, error) {
	p := &protobufProc{
		log: mgr.Logger(),
	}
	var err error
	if p.operator, err = strToProtobufOperator(mgr.FS(), conf.Operator, conf.Message, conf.ImportPaths); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *protobufProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	if err := p.operator(msg); err != nil {
		p.log.Debugf("Operator failed: %v", err)
		return nil, err
	}
	return []*message.Part{msg}, nil
}

func (p *protobufProc) Close(context.Context) error {
	return nil
}
