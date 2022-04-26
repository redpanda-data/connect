package processor

// LogConfig contains configuration fields for the Log processor.
type LogConfig struct {
	Level         string            `json:"level" yaml:"level"`
	Fields        map[string]string `json:"fields" yaml:"fields"`
	FieldsMapping string            `json:"fields_mapping" yaml:"fields_mapping"`
	Message       string            `json:"message" yaml:"message"`
}

// NewLogConfig returns a LogConfig with default values.
func NewLogConfig() LogConfig {
	return LogConfig{
		Level:         "INFO",
		Fields:        map[string]string{},
		FieldsMapping: "",
		Message:       "",
	}
}
