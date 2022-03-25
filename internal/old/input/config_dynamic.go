package input

// DynamicConfig contains configuration for the Dynamic input type.
type DynamicConfig struct {
	Inputs map[string]Config `json:"inputs" yaml:"inputs"`
	Prefix string            `json:"prefix" yaml:"prefix"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Inputs: map[string]Config{},
		Prefix: "",
	}
}
