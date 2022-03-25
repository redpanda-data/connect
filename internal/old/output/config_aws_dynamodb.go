package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

type sessionConfig struct {
	sess.Config `json:",inline" yaml:",inline"`
}

// DynamoDBConfig contains config fields for the DynamoDB output type.
type DynamoDBConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	Table          string            `json:"table" yaml:"table"`
	StringColumns  map[string]string `json:"string_columns" yaml:"string_columns"`
	JSONMapColumns map[string]string `json:"json_map_columns" yaml:"json_map_columns"`
	TTL            string            `json:"ttl" yaml:"ttl"`
	TTLKey         string            `json:"ttl_key" yaml:"ttl_key"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       policy.Config `json:"batching" yaml:"batching"`
}

// NewDynamoDBConfig creates a DynamoDBConfig populated with default values.
func NewDynamoDBConfig() DynamoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return DynamoDBConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		Table:          "",
		StringColumns:  map[string]string{},
		JSONMapColumns: map[string]string{},
		TTL:            "",
		TTLKey:         "",
		MaxInFlight:    64,
		Config:         rConf,
		Batching:       policy.NewConfig(),
	}
}
