package processor

import "github.com/benthosdev/benthos/v4/internal/httpclient/oldconfig"

// HTTPConfig contains configuration fields for the HTTP processor.
type HTTPConfig struct {
	BatchAsMultipart    bool `json:"batch_as_multipart" yaml:"batch_as_multipart"`
	Parallel            bool `json:"parallel" yaml:"parallel"`
	oldconfig.OldConfig `json:",inline" yaml:",inline"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		BatchAsMultipart: false,
		Parallel:         false,
		OldConfig:        oldconfig.NewOldConfig(),
	}
}
