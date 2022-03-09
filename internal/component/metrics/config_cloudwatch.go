package metrics

import (
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"
)

// CloudWatchConfig contains config fields for the CloudWatch metrics type.
type CloudWatchConfig struct {
	session.Config `json:",inline" yaml:",inline"`
	Namespace      string `json:"namespace" yaml:"namespace"`
	FlushPeriod    string `json:"flush_period" yaml:"flush_period"`
}

// NewCloudWatchConfig creates an CloudWatchConfig struct with default values.
func NewCloudWatchConfig() CloudWatchConfig {
	return CloudWatchConfig{
		Config:      session.NewConfig(),
		Namespace:   "Benthos",
		FlushPeriod: "100ms",
	}
}
