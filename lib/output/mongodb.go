package output

import (
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
)

// MongoDBConfig contains config fields for the MongoDB output type.
type MongoDBConfig struct {
	MongoConfig client.Config `json:",inline" yaml:",inline"`

	Operation    string              `json:"operation" yaml:"operation"`
	WriteConcern client.WriteConcern `json:"write_concern" yaml:"write_concern"`

	FilterMap   string `json:"filter_map" yaml:"filter_map"`
	DocumentMap string `json:"document_map" yaml:"document_map"`
	HintMap     string `json:"hint_map" yaml:"hint_map"`

	//DeleteEmptyValue bool `json:"delete_empty_value" yaml:"delete_empty_value"`
	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	RetryConfig retries.Config     `json:",inline" yaml:",inline"`
	Batching    batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewMongoDBConfig creates a MongoDB populated with default values.
func NewMongoDBConfig() MongoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return MongoDBConfig{
		MongoConfig:  client.NewConfig(),
		Operation:    "update-one",
		MaxInFlight:  1,
		RetryConfig:  rConf,
		Batching:     batch.NewPolicyConfig(),
		WriteConcern: client.WriteConcern{},
	}
}
