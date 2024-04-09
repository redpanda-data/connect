package batchconfig

import (
	"github.com/benthosdev/benthos/v4/internal/component/processor"
)

// Config contains configuration parameters for a batch policy.
type Config struct {
	ByteSize   int                `json:"byte_size" yaml:"byte_size"`
	Count      int                `json:"count" yaml:"count"`
	Check      string             `json:"check" yaml:"check"`
	Period     string             `json:"period" yaml:"period"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig creates a default PolicyConfig.
func NewConfig() Config {
	return Config{
		ByteSize:   0,
		Count:      0,
		Check:      "",
		Period:     "",
		Processors: []processor.Config{},
	}
}

// IsNoop returns true if this batch policy configuration does nothing.
func (p Config) IsNoop() bool {
	if p.ByteSize > 0 {
		return false
	}
	if p.Count > 1 {
		return false
	}
	if p.Check != "" {
		return false
	}
	if p.Period != "" {
		return false
	}
	if len(p.Processors) > 0 {
		return false
	}
	return true
}

// IsLimited returns true if there's any limit on the batching policy.
func (p Config) IsLimited() bool {
	if p.ByteSize > 0 {
		return true
	}
	if p.Count > 0 {
		return true
	}
	if p.Period != "" {
		return true
	}
	if p.Check != "" {
		return true
	}
	return false
}

// IsHardLimited returns true if there's a realistic limit on the batching
// policy, where checks are not included.
func (p Config) IsHardLimited() bool {
	if p.ByteSize > 0 {
		return true
	}
	if p.Count > 0 {
		return true
	}
	if p.Period != "" {
		return true
	}
	return false
}
