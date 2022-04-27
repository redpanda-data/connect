package processor

// AvroConfig contains configuration fields for the Avro processor.
type AvroConfig struct {
	Operator   string `json:"operator" yaml:"operator"`
	Encoding   string `json:"encoding" yaml:"encoding"`
	Schema     string `json:"schema" yaml:"schema"`
	SchemaPath string `json:"schema_path" yaml:"schema_path"`
}

// NewAvroConfig returns a AvroConfig with default values.
func NewAvroConfig() AvroConfig {
	return AvroConfig{
		Operator:   "",
		Encoding:   "textual",
		Schema:     "",
		SchemaPath: "",
	}
}
