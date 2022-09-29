package avro

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/linkedin/goavro/v2"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newAvro(conf.Avro, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("avro", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "avro",
		Categories: []string{
			"Parsing",
		},
		Summary: `
Performs Avro based operations on messages based on a schema.`,
		Status: docs.StatusBeta,
		Description: `
WARNING: If you are consuming or generating messages using a schema registry service then it is likely this processor will fail as those services require messages to be prefixed with the identifier of the schema version being used. Instead, try the ` + "[`schema_registry_encode`](/docs/components/processors/schema_registry_encode) and [`schema_registry_decode`](/docs/components/processors/schema_registry_decode)" + ` processors.

## Operators

### ` + "`to_json`" + `

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

### ` + "`from_json`" + `

Attempts to convert JSON documents into Avro documents according to the
specified encoding.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("operator", "The [operator](#operators) to execute").HasOptions("to_json", "from_json"),
			docs.FieldString("encoding", "An Avro encoding format to use for conversions to and from a schema.").HasOptions("textual", "binary", "single"),
			docs.FieldString("schema", "A full Avro schema to use."),
			docs.FieldString(
				"schema_path", "The path of a schema document to apply. Use either this or the `schema` field.",
				"file://path/to/spec.avsc",
				"http://localhost:8081/path/to/spec/versions/1",
			),
		).ChildDefaultAndTypesFromStruct(processor.NewAvroConfig()),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type avroOperator func(part *message.Part) error

func newAvroToJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromTextual(part.AsBytes())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(jObj)
			return nil
		}, nil
	case "binary":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromBinary(part.AsBytes())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(jObj)
			return nil
		}, nil
	case "single":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromSingle(part.AsBytes())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(jObj)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func newAvroFromJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *message.Part) error {
			jObj, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var textual []byte
			if textual, err = codec.TextualFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.SetBytes(textual)
			return nil
		}, nil
	case "binary":
		return func(part *message.Part) error {
			jObj, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var binary []byte
			if binary, err = codec.BinaryFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.SetBytes(binary)
			return nil
		}, nil
	case "single":
		return func(part *message.Part) error {
			jObj, err := part.AsStructured()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var single []byte
			if single, err = codec.SingleFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.SetBytes(single)
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func strToAvroOperator(opStr, encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch opStr {
	case "to_json":
		return newAvroToJSONOperator(encoding, codec)
	case "from_json":
		return newAvroFromJSONOperator(encoding, codec)
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

func loadSchema(schemaPath string) (string, error) {
	t := &http.Transport{}
	t.RegisterProtocol("file", http.NewFileTransport(http.Dir("/")))
	c := &http.Client{Transport: t}

	response, err := c.Get(schemaPath)
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

type avro struct {
	operator avroOperator
	log      log.Modular
}

func newAvro(conf processor.AvroConfig, mgr bundle.NewManagement) (processor.V2, error) {
	a := &avro{log: mgr.Logger()}

	var schema string
	var err error

	if schemaPath := conf.SchemaPath; schemaPath != "" {
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, fmt.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		schema, err = loadSchema(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load Avro schema definition: %v", err)
		}
	} else {
		schema = conf.Schema
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	if a.operator, err = strToAvroOperator(conf.Operator, conf.Encoding, codec); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

func (p *avro) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	err := p.operator(msg)
	if err != nil {
		p.log.Debugf("Operator failed: %v\n", err)
		return nil, err
	}
	return []*message.Part{msg}, nil
}

func (p *avro) Close(context.Context) error {
	return nil
}
