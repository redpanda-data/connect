package processor

import (
	ihttpdocs "github.com/benthosdev/benthos/v4/internal/http/docs"
)

// HTTPConfig contains configuration fields for the HTTP processor.
type HTTPConfig struct {
	BatchAsMultipart bool `json:"batch_as_multipart" yaml:"batch_as_multipart"`
	Parallel         bool `json:"parallel" yaml:"parallel"`
	ihttpdocs.Config `json:",inline" yaml:",inline"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		BatchAsMultipart: false,
		Parallel:         false,
		Config:           ihttpdocs.NewConfig(),
	}
}
