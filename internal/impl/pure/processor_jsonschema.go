package pure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"

	jsonschema "github.com/xeipuuv/gojsonschema"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newJSONSchema(conf.JSONSchema, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("json_schema", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "json_schema",
		Categories: []string{
			"Mapping",
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
    - mapping: 'root = deleted()' # Drop messages that fail
` + "```" + `

If a payload being processed looked like:

` + "```json" + `
{"firstName":"John","lastName":"Doe","age":-21}
` + "```" + `

Then a log message would appear explaining the fault and the payload would be
dropped.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("schema", "A schema to apply. Use either this or the `schema_path` field."),
			docs.FieldString("schema_path", "The path of a schema document to apply. Use either this or the `schema` field."),
		).ChildDefaultAndTypesFromStruct(processor.NewJSONSchemaConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type jsonSchemaProc struct {
	log    log.Modular
	schema *jsonschema.Schema
}

func newJSONSchema(conf processor.JSONSchemaConfig, mgr bundle.NewManagement) (processor.V2, error) {
	var schema *jsonschema.Schema
	var err error

	// load JSONSchema definition
	if schemaPath := conf.SchemaPath; schemaPath != "" {
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, fmt.Errorf("invalid schema_path provided, must start with file:// or http://")
		}

		schema, err = jsonschema.NewSchema(jsonschema.NewReferenceLoader(conf.SchemaPath))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else if conf.Schema != "" {
		schema, err = jsonschema.NewSchema(jsonschema.NewStringLoader(conf.Schema))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else {
		return nil, fmt.Errorf("either schema or schema_path must be provided")
	}

	return &jsonSchemaProc{
		log:    mgr.Logger(),
		schema: schema,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *jsonSchemaProc) Process(ctx context.Context, part *message.Part) ([]*message.Part, error) {
	jsonPart, err := part.AsStructured()
	if err != nil {
		s.log.Debugf("Failed to parse part into json: %v", err)
		return nil, err
	}

	partLoader := jsonschema.NewGoLoader(jsonPart)
	result, err := s.schema.Validate(partLoader)
	if err != nil {
		s.log.Debugf("Failed to validate json: %v", err)
		return nil, err
	}

	if !result.Valid() {
		s.log.Debugf("The document is not valid")
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
		return nil, errors.New(errStr)
	}

	s.log.Debugf("The document is valid")
	return []*message.Part{part}, nil
}

func (s *jsonSchemaProc) Close(context.Context) error {
	return nil
}
