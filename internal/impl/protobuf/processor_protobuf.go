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

// This file contains code originally licensed under the MIT License:

// Copyright (c) 2024-present Bento contributors

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package protobuf

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"

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

	// BSR Config
	fieldBSRConfig  = "bsr"
	fieldBSRModule  = "module"
	fieldBSRUrl     = "url"
	fieldBSRAPIKey  = "api_key"
	fieldBSRVersion = "version"
)

func protobufProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing").
		Summary(`
Performs conversions to or from a protobuf message. This processor uses reflection, meaning conversions can be made directly from the target .proto files.
`).Description(`
The main functionality of this processor is to map to and from JSON documents, you can read more about JSON mapping of protobuf messages here: [https://developers.google.com/protocol-buffers/docs/proto3#json](https://developers.google.com/protocol-buffers/docs/proto3#json)

Using reflection for processing protobuf messages in this way is less performant than generating and using native code. Therefore when performance is critical it is recommended that you use Redpanda Connect plugins instead for processing protobuf messages natively, you can find an example of Redpanda Connect plugins at [https://github.com/redpanda-data/redpanda-connect-plugin-example](https://github.com/redpanda-data/redpanda-connect-plugin-example)

The processor will ignore any files that begin with a dot ("."g), a convention for hidden files, when loading protocol buffer definitions.
== Operators

=== `+"`to_json`"+`

Converts protobuf messages into serialized proto3 JSON.

=== `+"`from_json`"+`

Attempts to create a target protobuf message from a serialized proto3 JSON.

=== `+"`decode`"+`

Converts protobuf messages into a generic structured message. This makes it easier to manipulate the contents of the document within Redpanda Connect.
This differs from `+"`to_json`"+` in the following ways:

- 64 bit numbers are *not* converted into strings
- Bytes and google.protobuf.Timestamp types are preserved (not encoded as strings unless serialized)

This operator is also considerably faster in scenario where you manipulate the data as the data does not need to be serialized then deserialized like with the `+"`to_json`"+` operator.
`).Fields(
		service.NewStringEnumField(fieldOperator, "to_json", "from_json", "decode").
			Description("The [operator](#operators) to execute"),
		service.NewStringField(fieldMessage).
			Description("The fully qualified name of the protobuf message to convert to/from."),
		service.NewBoolField(fieldDiscardUnknown).
			Description("If `true`, the `from_json` operator discards fields that are unknown to the schema.").
			Default(false),
		service.NewBoolField(fieldUseProtoNames).
			Description("If `true`, the `to_json` or `decode` operator deserializes fields exactly as named in schema file.").
			Default(false),
		service.NewStringListField(fieldImportPaths).
			Description("A list of directories containing .proto files, including all definitions required for parsing the target message. If left empty the current directory is used. Each directory listed will be walked with all found .proto files imported. Either this field or `bsr` must be populated.").
			Default([]string{}),
		service.NewBoolField(fieldUseEnumNumbers).
			Description("If `true`, the `to_json` or `decode` operator deserializes enums as numerical values instead of string names.").
			Default(false),
		service.NewObjectListField(fieldBSRConfig,
			service.NewStringField(fieldBSRModule).
				Description("Module to fetch from a Buf Schema Registry e.g. 'buf.build/exampleco/mymodule'."),
			service.NewStringField(fieldBSRUrl).
				Description("Buf Schema Registry URL, leave blank to extract from module.").
				Default("").Advanced(),
			service.NewStringField(fieldBSRAPIKey).
				Description("Buf Schema Registry server API key, can be left blank for a public registry.").
				Secret().
				Default(""),
			service.NewStringField(fieldBSRVersion).
				Description("Version to retrieve from the Buf Schema Registry, leave blank for latest.").
				Default("").Advanced(),
		).Description("Buf Schema Registry configuration. Either this field or `import_paths` must be populated. Note that this field is an array, and multiple BSR configurations can be provided.").
			Default([]any{}),
	).LintRule(`
root = match {
this.import_paths.type() == "unknown" && this.bsr.length() == 0 => [ "at least one of `+"`import_paths`"+`and `+"`bsr`"+` must be set" ],
this.import_paths.type() == "array" && this.import_paths.length() > 0 && this.bsr.length() > 0 => [ "both `+"`import_paths`"+` and `+"`bsr`"+` can't be set simultaneously" ],
}`).Example(
		"JSON to Protobuf using Schema from Disk", `
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
		"Protobuf to JSON using Schema from Disk", `
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
`).Example(
		"JSON to Protobuf using Buf Schema Registry", `
If we have the following protobuf definition within a BSR module hosted at `+"`buf.build/exampleco/mymodule`"+`:

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
        bsr:
          - module: buf.build/exampleco/mymodule
            api_key: xxx
`).Example(
		"Protobuf to JSON using Buf Schema Registry", `
If we have the following protobuf definition within a BSR module hosted at `+"`buf.build/exampleco/mymodule`"+`:
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
        bsr:
          - module: buf.build/exampleco/mymodule
            api_key: xxxx
`)
}

func init() {
	service.MustRegisterProcessor("protobuf", protobufProcessorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProtobuf(conf, mgr)
		})
}

type protobufOperator func(part *service.Message) error

func newProtobufToJSONOperator(
	f fs.FS,
	msg string,
	importPaths []string,
	toMessage common.ToMessageFn,
	opts protojson.MarshalOptions,
) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	fds, err := common.ParseFromFS(f, importPaths)
	if err != nil {
		return nil, fmt.Errorf("unable to load protos: %w", err)
	}
	_, types, err := common.BuildRegistries(fds)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve protobuf types: %w", err)
	}
	msgType, err := types.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find protobuf type %q: %w", msg, err)
	}
	decoder := common.NewDynamicPbDecoder(msgType.Descriptor())
	opts.Resolver = types
	return func(part *service.Message) error {
		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}
		return decoder.WithDecoded(partBytes, func(msg proto.Message) error {
			return toMessage(msg.ProtoReflect(), opts, part)
		})
	}, nil
}

func newProtobufFromJSONOperator(f fs.FS, msg string, importPaths []string, opts protojson.UnmarshalOptions) (protobufOperator, error) {
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

		opts.Resolver = types
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

func newProtobufToJSONBSROperator(
	multiModuleWatcher *multiModuleWatcher,
	msg string,
	toMessage common.ToMessageFn,
	opts protojson.MarshalOptions,
) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
	}
	decoder := common.NewDynamicPbDecoder(d.Descriptor())
	opts.Resolver = multiModuleWatcher
	return func(part *service.Message) error {
		partBytes, err := part.AsBytes()
		if err != nil {
			return err
		}
		return decoder.WithDecoded(partBytes, func(msg proto.Message) error {
			return toMessage(msg.ProtoReflect(), opts, part)
		})
	}, nil
}

func newProtobufFromJSONBSROperator(multiModuleWatcher *multiModuleWatcher, msg string, opts protojson.UnmarshalOptions) (protobufOperator, error) {
	if msg == "" {
		return nil, errors.New("message field must not be empty")
	}

	d, err := multiModuleWatcher.FindMessageByName(protoreflect.FullName(msg))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition: %w", msg, err)
	}

	opts.Resolver = multiModuleWatcher
	return func(part *service.Message) error {
		msgBytes, err := part.AsBytes()
		if err != nil {
			return err
		}
		dynMsg := dynamicpb.NewMessage(d.Descriptor())
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

type protojsonOptions struct {
	protojson.MarshalOptions
	protojson.UnmarshalOptions
}

func strToProtobufOperator(f fs.FS, opStr, message string, importPaths []string, opts protojsonOptions) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONOperator(f, message, importPaths, common.ToMessageSlow, opts.MarshalOptions)
	case "from_json":
		return newProtobufFromJSONOperator(f, message, importPaths, opts.UnmarshalOptions)
	case "decode":
		return newProtobufToJSONOperator(f, message, importPaths, common.ToMessageFast, opts.MarshalOptions)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func strToProtobufBSROperator(multiModuleWatcher *multiModuleWatcher, opStr, message string, opts protojsonOptions) (protobufOperator, error) {
	switch opStr {
	case "to_json":
		return newProtobufToJSONBSROperator(multiModuleWatcher, message, common.ToMessageSlow, opts.MarshalOptions)
	case "from_json":
		return newProtobufFromJSONBSROperator(multiModuleWatcher, message, opts.UnmarshalOptions)
	case "decode":
		return newProtobufToJSONBSROperator(multiModuleWatcher, message, common.ToMessageSlow, opts.MarshalOptions)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadDescriptors(f fs.FS, importPaths []string) (*protoregistry.Files, *protoregistry.Types, error) {
	files, err := common.ParseFromFS(f, importPaths)
	if err != nil {
		return nil, nil, err
	}
	return common.BuildRegistries(files)
}

//------------------------------------------------------------------------------

type protobufProc struct {
	operator protobufOperator
	log      *service.Logger
	// Used for loading and reading from multiple Buf Schema Registry repositories
	multiModuleWatcher *multiModuleWatcher
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

	var opts protojsonOptions

	if opts.DiscardUnknown, err = conf.FieldBool(fieldDiscardUnknown); err != nil {
		return nil, err
	}

	if opts.UseProtoNames, err = conf.FieldBool(fieldUseProtoNames); err != nil {
		return nil, err
	}

	if opts.UseEnumNumbers, err = conf.FieldBool(fieldUseEnumNumbers); err != nil {
		return nil, err
	}

	// Load BSR config
	var bsrModules []*service.ParsedConfig
	if bsrModules, err = conf.FieldObjectList(fieldBSRConfig); err != nil {
		return nil, err
	}

	// if BSR config is present, use BSR to discover proto definitions
	if len(bsrModules) > 0 {
		if p.multiModuleWatcher, err = newMultiModuleWatcher(bsrModules); err != nil {
			return nil, fmt.Errorf("failed to create multiModuleWatcher: %w", err)
		}
		if p.operator, err = strToProtobufBSROperator(p.multiModuleWatcher, operatorStr, message, opts); err != nil {
			return nil, err
		}
	} else {
		// else read from file paths
		var importPaths []string
		if importPaths, err = conf.FieldStringList(fieldImportPaths); err != nil {
			return nil, err
		}
		if p.operator, err = strToProtobufOperator(mgr.FS(), operatorStr, message, importPaths, opts); err != nil {
			return nil, err
		}
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
