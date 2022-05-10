package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

// KinesisConfig contains configuration fields for the Kinesis output type.
type KinesisConfig struct {
	SessionConfig  `json:",inline" yaml:",inline"`
	Stream         string `json:"stream" yaml:"stream"`
	HashKey        string `json:"hash_key" yaml:"hash_key"`
	PartitionKey   string `json:"partition_key" yaml:"partition_key"`
	MaxInFlight    int    `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return KinesisConfig{
		SessionConfig: SessionConfig{
			Config: sess.NewConfig(),
		},
		Stream:       "",
		HashKey:      "",
		PartitionKey: "",
		MaxInFlight:  64,
		Config:       rConf,
		Batching:     batchconfig.NewConfig(),
	}
}
