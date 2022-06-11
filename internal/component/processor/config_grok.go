package processor

// GrokConfig contains configuration fields for the Grok processor.
type GrokConfig struct {
	Expressions        []string          `json:"expressions" yaml:"expressions"`
	RemoveEmpty        bool              `json:"remove_empty_values" yaml:"remove_empty_values"`
	NamedOnly          bool              `json:"named_captures_only" yaml:"named_captures_only"`
	UseDefaults        bool              `json:"use_default_patterns" yaml:"use_default_patterns"`
	PatternPaths       []string          `json:"pattern_paths" yaml:"pattern_paths"`
	PatternDefinitions map[string]string `json:"pattern_definitions" yaml:"pattern_definitions"`
}

// NewGrokConfig returns a GrokConfig with default values.
func NewGrokConfig() GrokConfig {
	return GrokConfig{
		Expressions:        []string{},
		RemoveEmpty:        true,
		NamedOnly:          true,
		UseDefaults:        true,
		PatternPaths:       []string{},
		PatternDefinitions: make(map[string]string),
	}
}
