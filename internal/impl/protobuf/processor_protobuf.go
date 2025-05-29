// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protobuf

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/redpanda-data/benthos/v4/public/service"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	fieldOperator       = "operator"
	fieldMessage        = "message"
	fieldImportPaths    = "import_paths"
	fieldDiscardUnknown = "discard_unknown"
	fieldUseProtoNames  = "use_proto_names"
	fieldUseEnumNumbers = "use_enum_numbers"
)

func protobufProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing").
		Summary(`
Performs conversions to or from a protobuf message. This processor uses reflection, meaning conversions can be made directly from the target .proto files.
`).Description(`
The main functionality of this processor is to map to and from JSON documents, you can read more about JSON mapping of protobuf messages here: https://developers.google.com/protocol-buffers/docs/proto3#json[https://developers.google.com/protocol-buffers/docs/proto3#json^]

Using reflection for processing protobuf messages in this way is less performant than generating and using native code. Therefore when performance is critical it is recommended that you use Redpanda Connect plugins instead for processing protobuf messages natively, you can find an example of Redpanda Connect plugins at https://github.com/benthosdev/benthos-plugin-example[https://github.com/benthosdev/benthos-plugin-example^]

== Operators

=== `+"`to_json`"+`

Converts protobuf messages into a generic JSON structure. This makes it easier to manipulate the contents of the document within Benthos.

=== `+"`from_json`"+`

Attempts to create a target protobuf message from a generic JSON structure.
`).Fields(
		service.NewStringEnumField(fieldOperator, "to_json", "from_json").
			Description("The <<operators, operator>> to execute"),
		service.NewStringField(fieldMessage).
			Description("The fully qualified name of the protobuf message to convert to/from."),
		service.NewBoolField(fieldDiscardUnknown).
			Description("If `true`, the `from_json` operator discards fields that are unknown to the schema.").
			Default(false),
		service.NewBoolField(fieldUseProtoNames).
			Description("If `true`, the `to_json` operator deserializes fields exactly as named in schema file.").
			Default(false),
		service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported.").
			Default([]string{}),
		service.NewBoolField(fieldUseEnumNumbers).
			Description("If `true`, the `to_json` operator deserializes enums as numerical values instead of string names.").
			Default(false),
	).Example(
		"JSON to Protobuf", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
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
`+"```"+`

And a stream of JSON documents of the form:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

We can convert the documents into protobuf messages with the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: from_json
        message: testing.Person
        import_paths: [ testing/schema ]
`).Example(
		"Protobuf to JSON", `
If we have the following protobuf definition within a directory called `+"`testing/schema`"+`:

`+"```protobuf"+`
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
`+"```"+`

And a stream of protobuf messages of the type `+"`Person`"+`, we could convert them into JSON documents of the format:

`+"```json"+`
{
	"firstName": "caleb",
	"lastName": "quaye",
	"email": "caleb@myspace.com"
}
`+"```"+`

With the following config:`, `
pipeline:
  processors:
    - protobuf:
        operator: to_json
        message: testing.Person
        import_paths: [ testing/schema ]
`)
}

func init() {
	service.MustRegisterProcessor("protobuf", protobufProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProtobuf(conf, mgr)
		})
}

type protobufOperator func(part *service.Message) error

func newProtobufToJSONOperator(f fs.FS, msg string, importPaths []string, useProtoNames, useEnumNumbers bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	descriptors, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	d, err := descriptors.FindDescriptorByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("message descriptor %v was unexpected type %T", msg, d)
	}

	return func(part *service.Message) error {
		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(partBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message '%v': %w", msg, err)
		}

		opts := protojson.MarshalOptions{
			Resolver:       types,
			UseProtoNames:  useProtoNames,
			UseEnumNumbers: useEnumNumbers,
		}
		data, err := opts.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON protobuf message '%v': %w", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func newProtobufFromJSONOperator(f fs.FS, msg string, importPaths []string, discardUnknown bool) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	_, types, err := loadDescriptors(f, importPaths)
	if err != nil {
		return nil, err
	}

	types.RangeMessages(func(protoreflect.MessageType) bool {
		return true
	})

	md, err := types.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition within '%v'", msg, importPaths)
	}

	return func(part *service.Message) error {
		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}

		dynMsg := dynamicpb.NewMessage(md.Descriptor())

		opts := protojson.UnmarshalOptions{
			Resolver:       types,
			DiscardUnknown: discardUnknown,
		}
		if err := opts.Unmarshal(msgBytes, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal JSON message '%v': %w", msg, err)
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message '%v': %v", msg, err)
		}

		part.SetBytes(data)
		return nil
	}, nil
}

func strToProtobufOperator(f fs.FS, opStr, message string, importPaths []string, discardUnknown, useProtoNames, useEnumNumbers bool) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(f, message, importPaths, useProtoNames, useEnumNumbers)
	case "from_json":
		return newProtobufFromJSONOperator(f, message, importPaths, discardUnknown)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files := map[string]string{}
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
				content, ferr := os.ReadFile(path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return RegistriesFromMap(files)
}

//------------------------------------------------------------------------------

type protobufProc struct {
	operator protobufOperator
	log      *service.Logger
}

func newProtobuf(conf *service.ParsedConfig, mgr *service.Resources) (*protobufProc, error) {
	p := &protobufProc{
		log: mgr.Logger(),
	}

	operatorStr, err := conf.FieldString(fieldOperator)
	if err != nil {
		return nil, err
	}

	var message string
	if message, err = conf.FieldString(fieldMessage); err != nil {
		return nil, err
	}

	var importPaths []string
	if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
		return nil, err
	}

	var discardUnknown bool
	if discardUnknown, err = conf.FieldBool(fieldDiscardUnknown); err != nil {
		return nil, err
	}

	var useProtoNames bool
	if useProtoNames, err = conf.FieldBool(fieldUseProtoNames); err != nil {
		return nil, err
	}

	var useEnumNumbers bool
	if useEnumNumbers, err = conf.FieldBool(fieldUseEnumNumbers); err != nil {
		return nil, err
	}

	if p.operator, err = strToProtobufOperator(mgr.FS(), operatorStr, message, importPaths, discardUnknown, useProtoNames, useEnumNumbers); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *protobufProc) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	if err := p.operator(msg); err != nil {
		p.log.Debugf("Operator failed: %v", err)
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (*protobufProc) Close(context.Context) error {
	return nil
}
