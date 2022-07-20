package processor

// ProtobufConfig contains configuration fields for the Protobuf processor.
type ProtobufConfig struct {
	Operator    string   `json:"operator" yaml:"operator"`
	Message     string   `json:"message" yaml:"message"`
	ImportPaths []string `json:"import_paths" yaml:"import_paths"`
}

// NewProtobufConfig returns a ProtobufConfig with default values.
func NewProtobufConfig() ProtobufConfig {
	return ProtobufConfig{
		Operator:    "",
		Message:     "",
		ImportPaths: []string{},
	}
}
