package output

// DynamicConfig contains configuration fields for the Dynamic output type.
type DynamicConfig struct {
	Outputs map[string]Config `json:"outputs" yaml:"outputs"`
	Prefix  string            `json:"prefix" yaml:"prefix"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Outputs: map[string]Config{},
		Prefix:  "",
	}
}
