package output

import (
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

// RetryConfig contains configuration values for the Retry output type.
type RetryConfig struct {
	Output         *Config `json:"output" yaml:"output"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewRetryConfig creates a new RetryConfig with default values.
func NewRetryConfig() RetryConfig {
	return RetryConfig{
		Output: nil,
		Config: retries.NewConfig(),
	}
}
