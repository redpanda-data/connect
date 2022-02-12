package processor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"

	jsonschema "github.com/xeipuuv/gojsonschema"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSONSchema] = TypeSpec{
		constructor: NewJSONSchema,
		Categories: []Category{
			CategoryMapping,
		},
		Summary: `
Checks messages against a provided JSONSchema definition but does not change the
payload under any circumstances. If a message does not match the schema it can
be caught using error handling methods outlined [here](/docs/configuration/error_handling).`,
		Description: `
Please refer to the [JSON Schema website](https://json-schema.org/) for
information and tutorials regarding the syntax of the schema.`,
		Footnotes: `
## Examples

With the following JSONSchema document:

` + "```json" + `
{
	"$id": "https://example.com/person.schema.json",
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Person",
	"type": "object",
	"properties": {
	  "firstName": {
		"type": "string",
		"description": "The person's first name."
	  },
	  "lastName": {
		"type": "string",
		"description": "The person's last name."
	  },
	  "age": {
		"description": "Age in years which must be equal to or greater than zero.",
		"type": "integer",
		"minimum": 0
	  }
	}
}
` + "```" + `

And the following Benthos configuration:

` + "```yaml" + `
pipeline:
  processors:
  - json_schema:
      schema_path: "file://path_to_schema.json"
  - catch:
    - log:
        level: ERROR
        message: "Schema validation failed due to: ${!error()}"
    - bloblang: 'root = deleted()' # Drop messages that fail
` + "```" + `

If a payload being processed looked like:

` + "```json" + `
{"firstName":"John","lastName":"Doe","age":-21}
` + "```" + `

Then a log message would appear explaining the fault and the payload would be
dropped.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("schema", "A schema to apply. Use either this or the `schema_path` field."),
			docs.FieldCommon("schema_path", "The path of a schema document to apply. Use either this or the `schema` field."),
			PartsFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// JSONSchemaConfig is a configuration struct containing fields for the
// jsonschema processor.
type JSONSchemaConfig struct {
	Parts      []int  `json:"parts" yaml:"parts"`
	SchemaPath string `json:"schema_path" yaml:"schema_path"`
	Schema     string `json:"schema" yaml:"schema"`
}

// NewJSONSchemaConfig returns a JSONSchemaConfig with default values.
func NewJSONSchemaConfig() JSONSchemaConfig {
	return JSONSchemaConfig{
		Parts:      []int{},
		SchemaPath: "",
		Schema:     "",
	}
}

//------------------------------------------------------------------------------

// JSONSchema is a processor that validates messages against a specified json schema.
type JSONSchema struct {
	conf   JSONSchemaConfig
	stats  metrics.Type
	log    log.Modular
	schema *jsonschema.Schema

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewJSONSchema returns a JSONSchema processor.
func NewJSONSchema(
	conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	var schema *jsonschema.Schema
	var err error

	// load JSONSchema definition
	if schemaPath := conf.JSONSchema.SchemaPath; schemaPath != "" {
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, fmt.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		schema, err = jsonschema.NewSchema(jsonschema.NewReferenceLoader(conf.JSONSchema.SchemaPath))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else if conf.JSONSchema.Schema != "" {
		schema, err = jsonschema.NewSchema(jsonschema.NewStringLoader(conf.JSONSchema.Schema))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else {
		return nil, fmt.Errorf("either schema or schema_path must be provided")
	}

	return &JSONSchema{
		stats:  stats,
		log:    log,
		schema: schema,

		mCount:     stats.GetCounter("count"),
		mErrJSONP:  stats.GetCounter("error_json_parse"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *JSONSchema) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	s.mCount.Incr(1)
	newMsg := msg.Copy()
	proc := func(i int, span *tracing.Span, part *message.Part) error {
		jsonPart, err := msg.Get(i).JSON()
		if err != nil {
			s.log.Debugf("Failed to parse part into json: %v\n", err)
			s.mErrJSONP.Incr(1)
			s.mErr.Incr(1)
			return err
		}

		partLoader := jsonschema.NewGoLoader(jsonPart)
		result, err := s.schema.Validate(partLoader)
		if err != nil {
			s.log.Debugf("Failed to validate json: %v\n", err)
			s.mErr.Incr(1)
			return err
		}

		if !result.Valid() {
			s.log.Debugf("The document is not valid\n")
			s.mErr.Incr(1)
			var errStr string
			for i, desc := range result.Errors() {
				if i > 0 {
					errStr += "\n"
				}
				description := strings.ToLower(desc.Description())
				if property := desc.Details()["property"]; property != nil {
					description = property.(string) + strings.TrimPrefix(description, strings.ToLower(property.(string)))
				}
				errStr += desc.Field() + " " + description
			}
			return errors.New(errStr)
		}
		s.log.Debugf("The document is valid\n")

		return nil
	}

	if newMsg.Len() == 0 {
		return nil, nil
	}

	IteratePartsWithSpanV2(TypeJSONSchema, s.conf.Parts, newMsg, proc)

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]*message.Batch{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *JSONSchema) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *JSONSchema) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
