package pure

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"

	jsonschema "github.com/xeipuuv/gojsonschema"
)

const (
	jschemaPFieldSchemaPath = "schema_path"
	jschemaPFieldSchema     = "schema"
)

func jschemaProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Stable().
		Summary(`Checks messages against a provided JSONSchema definition but does not change the payload under any circumstances. If a message does not match the schema it can be caught using error handling methods outlined [here](/docs/configuration/error_handling).`).
		Description(`Please refer to the [JSON Schema website](https://json-schema.org/) for information and tutorials regarding the syntax of the schema.`).
		Footnotes(`
## Examples

With the following JSONSchema document:

`+"```json"+`
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
`+"```"+`

And the following Benthos configuration:

`+"```yaml"+`
pipeline:
  processors:
  - json_schema:
      schema_path: "file://path_to_schema.json"
  - catch:
    - log:
        level: ERROR
        message: "Schema validation failed due to: ${!error()}"
    - mapping: 'root = deleted()' # Drop messages that fail
`+"```"+`

If a payload being processed looked like:

`+"```json"+`
{"firstName":"John","lastName":"Doe","age":-21}
`+"```"+`

Then a log message would appear explaining the fault and the payload would be
dropped.`).
		Fields(
			service.NewStringField(jschemaPFieldSchema).
				Description("A schema to apply. Use either this or the `schema_path` field.").
				Optional(),
			service.NewStringField(jschemaPFieldSchemaPath).
				Description("The path of a schema document to apply. Use either this or the `schema` field.").
				Optional(),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"json_schema", jschemaProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			schemaStr, _ := conf.FieldString(jschemaPFieldSchema)
			schemaPath, _ := conf.FieldString(jschemaPFieldSchemaPath)
			mgr := interop.UnwrapManagement(res)
			p, err := newJSONSchema(schemaStr, schemaPath, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("json_schema", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type jsonSchemaProc struct {
	log    log.Modular
	schema *jsonschema.Schema
}

func newJSONSchema(schemaStr, schemaPath string, mgr bundle.NewManagement) (processor.AutoObserved, error) {
	var schema *jsonschema.Schema
	var err error

	// load JSONSchema definition
	if schemaPath := schemaPath; schemaPath != "" {
		if !(strings.HasPrefix(schemaPath, "file://") || strings.HasPrefix(schemaPath, "http://")) {
			return nil, errors.New("invalid schema_path provided, must start with file:// or http://")
		}

		schema, err = jsonschema.NewSchema(jsonschema.NewReferenceLoaderFileSystem(schemaPath, ifs.ToHTTP(mgr.FS())))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else if schemaStr != "" {
		schema, err = jsonschema.NewSchema(jsonschema.NewStringLoader(schemaStr))
		if err != nil {
			return nil, fmt.Errorf("failed to load JSON schema definition: %v", err)
		}
	} else {
		return nil, errors.New("either schema or schema_path must be provided")
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
		s.log.Debug("Failed to parse part into json: %v", err)
		return nil, err
	}

	partLoader := jsonschema.NewGoLoader(jsonPart)
	result, err := s.schema.Validate(partLoader)
	if err != nil {
		s.log.Debug("Failed to validate json: %v", err)
		return nil, err
	}

	if !result.Valid() {
		s.log.Debug("The document is not valid")
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

	s.log.Debug("The document is valid")
	return []*message.Part{part}, nil
}

func (s *jsonSchemaProc) Close(context.Context) error {
	return nil
}
