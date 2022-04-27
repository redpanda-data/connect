package processor

// ParseLogConfig contains configuration fields for the ParseLog processor.
type ParseLogConfig struct {
	Format       string `json:"format" yaml:"format"`
	Codec        string `json:"codec" yaml:"codec"`
	BestEffort   bool   `json:"best_effort" yaml:"best_effort"`
	WithRFC3339  bool   `json:"allow_rfc3339" yaml:"allow_rfc3339"`
	WithYear     string `json:"default_year" yaml:"default_year"`
	WithTimezone string `json:"default_timezone" yaml:"default_timezone"`
}

// NewParseLogConfig returns a ParseLogConfig with default values.
func NewParseLogConfig() ParseLogConfig {
	return ParseLogConfig{
		Format: "",
		Codec:  "",

		BestEffort:   true,
		WithRFC3339:  true,
		WithYear:     "current",
		WithTimezone: "UTC",
	}
}
