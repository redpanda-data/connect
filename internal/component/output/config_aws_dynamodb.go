package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

// SessionConfig hides a general AWS session config struct.
type SessionConfig struct {
	sess.Config `json:",inline" yaml:",inline"`
}

// DynamoDBConfig contains config fields for the DynamoDB output type.
type DynamoDBConfig struct {
	SessionConfig  `json:",inline" yaml:",inline"`
	Table          string            `json:"table" yaml:"table"`
	StringColumns  map[string]string `json:"string_columns" yaml:"string_columns"`
	JSONMapColumns map[string]string `json:"json_map_columns" yaml:"json_map_columns"`
	TTL            string            `json:"ttl" yaml:"ttl"`
	TTLKey         string            `json:"ttl_key" yaml:"ttl_key"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewDynamoDBConfig creates a DynamoDBConfig populated with default values.
func NewDynamoDBConfig() DynamoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return DynamoDBConfig{
		SessionConfig: SessionConfig{
			Config: sess.NewConfig(),
		},
		Table:          "",
		StringColumns:  map[string]string{},
		JSONMapColumns: map[string]string{},
		TTL:            "",
		TTLKey:         "",
		MaxInFlight:    64,
		Config:         rConf,
		Batching:       batchconfig.NewConfig(),
	}
}
