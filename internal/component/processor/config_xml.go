package processor

// XMLConfig contains configuration fields for the XML processor.
type XMLConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Cast     bool   `json:"cast" yaml:"cast"`
}

// NewXMLConfig returns a XMLConfig with default values.
func NewXMLConfig() XMLConfig {
	return XMLConfig{
		Operator: "",
		Cast:     false,
	}
}
