package metrics

// JSONAPIConfig contains configuration parameters for the JSON API metrics aggregator.
type JSONAPIConfig struct{}

// NewJSONAPIConfig returns a new JSONAPIConfig with default values.
func NewJSONAPIConfig() JSONAPIConfig {
	return JSONAPIConfig{}
}
