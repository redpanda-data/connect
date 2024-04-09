package avro

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/linkedin/goavro/v2"

	"github.com/benthosdev/benthos/v4/public/service"
)

func avroConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary(`Performs Avro based operations on messages based on a schema.`).
		Description(`
WARNING: If you are consuming or generating messages using a schema registry service then it is likely this processor will fail as those services require messages to be prefixed with the identifier of the schema version being used. Instead, try the ` + "[`schema_registry_encode`](/docs/components/processors/schema_registry_encode) and [`schema_registry_decode`](/docs/components/processors/schema_registry_decode)" + ` processors.

## Operators

### ` + "`to_json`" + `

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

### ` + "`from_json`" + `

Attempts to convert JSON documents into Avro documents according to the
specified encoding.`).
		Field(service.NewStringEnumField("operator", "to_json", "from_json").Description("The [operator](#operators) to execute")).
		Field(service.NewStringEnumField("encoding", "textual", "binary", "single").Description("An Avro encoding format to use for conversions to and from a schema.").Default("textual")).
		Field(service.NewStringField("schema").Description("A full Avro schema to use.").Default("")).
		Field(service.NewStringField("schema_path").
			Description("The path of a schema document to apply. Use either this or the `schema` field.").
			Default("").
			Example("file://path/to/spec.avsc").
			Example("http://localhost:8081/path/to/spec/versions/1"))
}

func init() {
	err := service.RegisterProcessor("avro", avroConfigSpec(), newAvroFromConfig)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type avroOperator func(part *service.Message) error

func newAvroToJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			jObj, _, err := codec.NativeFromTextual(pBytes)
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(jObj)
			return nil
		}, nil
	case "binary":
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			jObj, _, err := codec.NativeFromBinary(pBytes)
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			part.SetStructuredMut(jObj)
			return nil
		}, nil
	case "single":
		return func(part *service.Message) error {
			pBytes, err := part.AsBytes()
			if err != nil {
				return err
			}
			jObj, _, err := codec.NativeFromSingle(pBytes)
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
		return func(part *service.Message) error {
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
		return func(part *service.Message) error {
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
		return func(part *service.Message) error {
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

type avro struct {
	operator avroOperator
	log      *service.Logger
}

func newAvroFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	a := &avro{log: mgr.Logger()}

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
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, errors.New("invalid schema_path provided, must start with file:// or http://")
		}
		if schema, err = loadSchema(schemaPath); err != nil {
			return nil, fmt.Errorf("failed to load Avro schema definition: %v", err)
		}
	}
	if schema == "" {
		return nil, errors.New("a schema must be specified with either the `schema` or `schema_path` fields")
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	if a.operator, err = strToAvroOperator(operator, encoding, codec); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

func (p *avro) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	err := p.operator(msg)
	if err != nil {
		p.log.Debugf("Operator failed: %v\n", err)
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (p *avro) Close(context.Context) error {
	return nil
}
