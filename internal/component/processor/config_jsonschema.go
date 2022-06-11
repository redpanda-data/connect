package processor

// JSONSchemaConfig is a configuration struct containing fields for the
// jsonschema processor.
type JSONSchemaConfig struct {
	SchemaPath string `json:"schema_path" yaml:"schema_path"`
	Schema     string `json:"schema" yaml:"schema"`
}

// NewJSONSchemaConfig returns a JSONSchemaConfig with default values.
func NewJSONSchemaConfig() JSONSchemaConfig {
	return JSONSchemaConfig{
		SchemaPath: "",
		Schema:     "",
	}
}
