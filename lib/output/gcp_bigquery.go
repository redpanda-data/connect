package output

import (
	"github.com/Jeffail/benthos/v3/lib/message/batch"
)

// GCPBigQueryConfig contains configuration fields for the GCP BigQuery
// output type.
type GCPBigQueryConfig struct {
	ProjectID   string             `json:"project" yaml:"project"`
	DatasetID   string             `json:"dataset" yaml:"dataset"`
	TableID     string             `json:"table" yaml:"table"`
	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching    batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewGCPBigQueryConfig creates a new Config with default values.
func NewGCPBigQueryConfig() GCPBigQueryConfig {
	return GCPBigQueryConfig{
		ProjectID:   "",
		DatasetID:   "",
		TableID:     "",
		MaxInFlight: 1,
		Batching:    batch.NewPolicyConfig(),
	}
}
