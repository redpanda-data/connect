package input

import (
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
)

// AWSSQSConfig contains configuration values for the input type.
type AWSSQSConfig struct {
	sess.Config         `json:",inline" yaml:",inline"`
	URL                 string `json:"url" yaml:"url"`
	WaitTimeSeconds     int    `json:"wait_time_seconds" yaml:"wait_time_seconds"`
	DeleteMessage       bool   `json:"delete_message" yaml:"delete_message"`
	ResetVisibility     bool   `json:"reset_visibility" yaml:"reset_visibility"`
	MaxNumberOfMessages int    `json:"max_number_of_messages" yaml:"max_number_of_messages"`
}

// NewAWSSQSConfig creates a new Config with default values.
func NewAWSSQSConfig() AWSSQSConfig {
	return AWSSQSConfig{
		Config:              sess.NewConfig(),
		URL:                 "",
		WaitTimeSeconds:     0,
		DeleteMessage:       true,
		ResetVisibility:     true,
		MaxNumberOfMessages: 10,
	}
}
