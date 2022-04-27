package processor

// JMESPathConfig contains configuration fields for the JMESPath processor.
type JMESPathConfig struct {
	Query string `json:"query" yaml:"query"`
}

// NewJMESPathConfig returns a JMESPathConfig with default values.
func NewJMESPathConfig() JMESPathConfig {
	return JMESPathConfig{
		Query: "",
	}
}
