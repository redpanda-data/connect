package processor

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/linkedin/goavro/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAvro] = TypeSpec{
		constructor: NewAvro,
		Categories: []Category{
			CategoryParsing,
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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The [operator](#operators) to execute").HasOptions("to_json", "from_json"),
			docs.FieldCommon("encoding", "An Avro encoding format to use for conversions to and from a schema.").HasOptions("textual", "binary", "single"),
			docs.FieldCommon("schema", "A full Avro schema to use."),
			docs.FieldCommon(
				"schema_path", "The path of a schema document to apply. Use either this or the `schema` field.",
				"file://path/to/spec.avsc",
				"http://localhost:8081/path/to/spec/versions/1",
			),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// AvroConfig contains configuration fields for the Avro processor.
type AvroConfig struct {
	Parts      []int  `json:"parts" yaml:"parts"`
	Operator   string `json:"operator" yaml:"operator"`
	Encoding   string `json:"encoding" yaml:"encoding"`
	Schema     string `json:"schema" yaml:"schema"`
	SchemaPath string `json:"schema_path" yaml:"schema_path"`
}

// NewAvroConfig returns a AvroConfig with default values.
func NewAvroConfig() AvroConfig {
	return AvroConfig{
		Parts:      []int{},
		Operator:   "to_json",
		Encoding:   "textual",
		Schema:     "",
		SchemaPath: "",
	}
}

//------------------------------------------------------------------------------

type avroOperator func(part *message.Part) error

func newAvroToJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromTextual(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	case "binary":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromBinary(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	case "single":
		return func(part *message.Part) error {
			jObj, _, err := codec.NativeFromSingle(part.Get())
			if err != nil {
				return fmt.Errorf("failed to convert Avro document to JSON: %v", err)
			}
			if err = part.SetJSON(jObj); err != nil {
				return fmt.Errorf("failed to set JSON: %v", err)
			}
			return nil
		}, nil
	}
	return nil, fmt.Errorf("encoding '%v' not recognised", encoding)
}

func newAvroFromJSONOperator(encoding string, codec *goavro.Codec) (avroOperator, error) {
	switch encoding {
	case "textual":
		return func(part *message.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var textual []byte
			if textual, err = codec.TextualFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(textual)
			return nil
		}, nil
	case "binary":
		return func(part *message.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var binary []byte
			if binary, err = codec.BinaryFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(binary)
			return nil
		}, nil
	case "single":
		return func(part *message.Part) error {
			jObj, err := part.JSON()
			if err != nil {
				return fmt.Errorf("failed to parse message as JSON: %v", err)
			}
			var single []byte
			if single, err = codec.SingleFromNative(nil, jObj); err != nil {
				return fmt.Errorf("failed to convert JSON to Avro schema: %v", err)
			}
			part.Set(single)
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

// Avro is a processor that performs an operation on an Avro payload.
type Avro struct {
	parts    []int
	operator avroOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewAvro returns an Avro processor.
func NewAvro(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	a := &Avro{
		parts: conf.Avro.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var schema string
	var err error

	if schemaPath := conf.Avro.SchemaPath; schemaPath != "" {
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, fmt.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		schema, err = loadSchema(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load Avro schema definition: %v", err)
		}
	} else {
		schema = conf.Avro.Schema
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	if a.operator, err = strToAvroOperator(conf.Avro.Operator, conf.Avro.Encoding, codec); err != nil {
		return nil, err
	}
	return a, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Avro) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	p.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part *message.Part) error {
		if err := p.operator(part); err != nil {
			p.mErr.Incr(1)
			p.log.Debugf("Operator failed: %v\n", err)
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2(TypeAvro, p.parts, newMsg, proc)

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	return []*message.Batch{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Avro) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *Avro) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
