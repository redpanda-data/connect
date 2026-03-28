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

package avro

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/twmb/avro"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func avroConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary(`Performs Avro based operations on messages based on a schema.`).
		Description(`
WARNING: If you are consuming or generating messages using a schema registry service then it is likely this processor will fail as those services require messages to be prefixed with the identifier of the schema version being used. Instead, try the ` + "xref:components:processors/schema_registry_encode.adoc[`schema_registry_encode`] and xref:components:processors/schema_registry_decode.adoc[`schema_registry_decode`]" + ` processors.

== Operators

=== ` + "`to_json`" + `

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

=== ` + "`from_json`" + `

Attempts to convert JSON documents into Avro documents according to the
specified encoding.`).
		Field(service.NewStringEnumField("operator", "to_json", "from_json").Description("The <<operators, operator>> to execute")).
		Field(service.NewStringEnumField("encoding", "textual", "binary", "single").Description("An Avro encoding format to use for conversions to and from a schema.").Default("textual")).
		Field(service.NewStringField("schema").Description("A full Avro schema to use.").Default("")).
		Field(service.NewStringField("schema_path").
			Description("The path of a schema document to apply. Use either this or the `schema` field. URLs must begin with `file://` or `http://`. Note that `file://` URLs must use absolute paths (e.g. `file:///absolute/path/to/spec.avsc`); relative paths are not supported.").
			Default("").
			Example("file:///path/to/spec.avsc").
			Example("http://localhost:8081/path/to/spec/versions/1"))
}

func init() {
	service.MustRegisterProcessor("avro", avroConfigSpec(), newAvroFromConfig)
}

//------------------------------------------------------------------------------

type avroOperator func(part *service.Message) error

func newAvroToJSONOperator(encoding string, schema *avro.Schema) (avroOperator, error) {
	decodeOpts := []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()}
	switch encoding {
	case "textual":
		// Input is Avro JSON bytes. Decode validates against the schema
		// and produces structured data with tagged unions.
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			var native any
			if err := schema.DecodeJSON(pBytes, &native, decodeOpts...); err != nil {
				return fmt.Errorf("converting Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(native)
			return nil
		}, nil
	case "binary":
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			var native any
			if _, err := schema.Decode(pBytes, &native, decodeOpts...); err != nil {
				return fmt.Errorf("converting Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(native)
			return nil
		}, nil
	case "single":
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			var native any
			if _, err := schema.DecodeSingleObject(pBytes, &native, decodeOpts...); err != nil {
				return fmt.Errorf("converting Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(native)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func newAvroFromJSONOperator(encoding string, schema *avro.Schema) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *service.Message) error {
			data, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("parsing message as JSON: %v", err)
			}
			textual, err := schema.EncodeJSON(data, avro.TaggedUnions(), avro.TagLogicalTypes(), avro.LinkedinFloats())
			if err != nil {
				return fmt.Errorf("converting JSON to Avro schema: %v", err)
			}
			part.SetBytes(textual)
			return nil
		}, nil
	case "binary":
		// Encode accepts both bare and tagged union maps, so
		// structured data from JSON input encodes directly.
		return func(part *service.Message) error {
			data, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("parsing message as structured: %v", err)
			}
			binary, err := schema.Encode(data)
			if err != nil {
				return fmt.Errorf("converting JSON to Avro schema: %v", err)
			}
			part.SetBytes(binary)
			return nil
		}, nil
	case "single":
		return func(part *service.Message) error {
			data, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("parsing message as structured: %v", err)
			}
			single, err := schema.AppendSingleObject(nil, data)
			if err != nil {
				return fmt.Errorf("converting JSON to Avro schema: %v", err)
			}
			part.SetBytes(single)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func strToAvroOperator(opStr, encoding string, schema *avro.Schema) (avroOperator, error) {
	switch opStr {
	case "to_json":
		return newAvroToJSONOperator(encoding, schema)
	case "from_json":
		return newAvroFromJSONOperator(encoding, schema)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadSchema(schemaPath string) (string, error) {
	t := &http.Transport{}
	t.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	c := &http.Client{Transport: t}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, schemaPath, http.NoBody)
	if err != nil {
		return "", err
	}

	response, err := c.Do(req)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

//------------------------------------------------------------------------------

type avroProcessor struct {
	operator avroOperator
	log      *service.Logger
}

func newAvroFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	a := &avroProcessor{log: mgr.Logger()}

	var operator, encoding, schema, schemaPath string
	var err error

	if operator, err = conf.FieldString("operator"); err != nil {
		return nil, err
	}
	if encoding, err = conf.FieldString("encoding"); err != nil {
		return nil, err
	}
	if schemaPath, err = conf.FieldString("schema_path"); err != nil {
		return nil, err
	}
	if schema, err = conf.FieldString("schema"); err != nil {
		return nil, err
	}
	if schemaPath != "" {
		if !strings.HasPrefix(schemaPath, "file://") && !strings.HasPrefix(schemaPath, "http://") {
			return nil, errors.New("invalid schema_path provided, must start with file:// or http://")
		}
		if schema, err = loadSchema(schemaPath); err != nil {
			return nil, fmt.Errorf("loading Avro schema definition: %v", err)
		}
	}
	if schema == "" {
		return nil, errors.New("a schema must be specified with either the `schema` or `schema_path` fields")
	}

	parsed, err := avro.Parse(schema)
	if err != nil {
		return nil, fmt.Errorf("parsing schema: %v", err)
	}

	if a.operator, err = strToAvroOperator(operator, encoding, parsed); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

func (p *avroProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	err := p.operator(msg)
	if err != nil {
		p.log.Debugf("Operator failed: %v\n", err)
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (*avroProcessor) Close(context.Context) error {
	return nil
}
