package condition

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	jsonschema "github.com/xeipuuv/gojsonschema"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSONSchema] = TypeSpec{
		constructor: NewJSONSchema,
		Description: `
Validates a message against the provided JSONSchema definition to retrieve a
boolean response indicating whether the message matches the schema or not.
If the response is true the condition passes, otherwise it does not. Please
refer to the [JSON Schema website](https://json-schema.org/) for information and
tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

` + "``` json" + `
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

` + "``` yaml" + `
json_schema:
  part: 0
  schema_path: "file://path_to_schema.json"
` + "```" + `

If the message being processed looked like:

` + "``` json" + `
{"firstName":"John","lastName":"Doe","age":21}
` + "```" + `

Then the condition would pass.`,
	}
}

//------------------------------------------------------------------------------

// JSONSchemaConfig is a configuration struct containing fields for the
// jsonschema condition.
type JSONSchemaConfig struct {
	Part       int    `json:"part" yaml:"part"`
	SchemaPath string `json:"schema_path" yaml:"schema_path"`
	Schema     string `json:"schema" yaml:"schema"`
}

// NewJSONSchemaConfig returns a JSONSchemaConfig with default values.
func NewJSONSchemaConfig() JSONSchemaConfig {
	return JSONSchemaConfig{
		Part:       0,
		SchemaPath: "",
		Schema:     "",
	}
}

//------------------------------------------------------------------------------

// JSONSchema is a condition that validates messages a specified JSONSchema.
type JSONSchema struct {
	stats  metrics.Type
	log    log.Modular
	part   int
	schema *jsonschema.Schema

	mCount    metrics.StatCounter
	mTrue     metrics.StatCounter
	mFalse    metrics.StatCounter
	mErrJSONP metrics.StatCounter
	mErr      metrics.StatCounter
}

// NewJSONSchema returns a JSONSchema condition.
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
		part:   conf.JSONSchema.Part,
		schema: schema,

		mCount:    stats.GetCounter("count"),
		mTrue:     stats.GetCounter("true"),
		mFalse:    stats.GetCounter("false"),
		mErrJSONP: stats.GetCounter("error_json_parse"),
		mErr:      stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *JSONSchema) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	index := c.part
	if index < 0 {
		index = msg.Len() + index
	}

	if index < 0 || index >= msg.Len() {
		c.mFalse.Incr(1)
		return false
	}

	jsonPart, err := msg.Get(index).JSON()
	if err != nil {
		c.log.Debugf("Failed to parse part into json: %v\n", err)
		c.mErrJSONP.Incr(1)
		c.mErr.Incr(1)
		c.mFalse.Incr(1)
		return false
	}

	partLoader := jsonschema.NewGoLoader(jsonPart)
	res, err := c.schema.Validate(partLoader)
	if err != nil {
		c.log.Debugf("Failed to validate json: %v\n", err)
		c.mErr.Incr(1)
		c.mFalse.Incr(1)
		return false
	}

	if res.Valid() {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}

	return res.Valid()
}

//------------------------------------------------------------------------------
