package input

// GenerateConfig contains configuration for the Bloblang input type.
type GenerateConfig struct {
	Mapping string `json:"mapping" yaml:"mapping"`
	// internal can be both duration string or cron expression
	Interval string `json:"interval" yaml:"interval"`
	Count    int    `json:"count" yaml:"count"`
}

// NewGenerateConfig creates a new BloblangConfig with default values.
func NewGenerateConfig() GenerateConfig {
	return GenerateConfig{
		Mapping:  "",
		Interval: "1s",
		Count:    0,
	}
}
