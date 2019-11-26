// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	jsonschema "github.com/xeipuuv/gojsonschema"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSONSchema] = TypeSpec{
		constructor: NewJSONSchema,
		description: `
Checks messages against a provided JSONSchema definition but does not change the
payload under any circumstances. If a message does not match the schema it can
be caught using error handling methods outlined [here](../error_handling.md).

Please refer to the [JSON Schema website](https://json-schema.org/) for
information and tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

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
        message: "Schema validation failed due to: ${!error}"
` + "```" + `

If a payload being processed looked like:

` + "```json" + `
{"firstName":"John","lastName":"Doe","age":-21}
` + "```" + `

Then the payload would be unchanged but a log message would appear explaining
the fault. This gives you flexibility in how you may handle schema errors, but
for a simpler use case you might instead wish to use the
` + "[`json_schema`](../conditions/README.md#json_schema)" + ` condition with a
` + "[`filter`](#filter)" + `.`,
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
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
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
func (s *JSONSchema) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	newMsg := msg.Copy()
	proc := func(i int, span opentracing.Span, part types.Part) error {
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
					errStr = errStr + "\n"
				}
				errStr = errStr + desc.Field() + " " + strings.ToLower(desc.Description())
			}
			return errors.New(errStr)
		}
		s.log.Debugf("The document is valid\n")

		return nil
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	IteratePartsWithSpan(TypeJSONSchema, s.conf.Parts, newMsg, proc)

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
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
