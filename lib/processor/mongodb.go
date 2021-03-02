package processor

import (
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
)

// MongoDBConfig contains configuration fields for the MongoDB processor.
type MongoDBConfig struct {
	MongoDB      client.Config       `json:",inline" yaml:",inline"`
	WriteConcern client.WriteConcern `json:"write_concern" yaml:"write_concern"`

	Parts       []int          `json:"parts" yaml:"parts"`
	Operation   string         `json:"operation" yaml:"operation"`
	FilterMap   string         `json:"filter_map" yaml:"filter_map"`
	DocumentMap string         `json:"document_map" yaml:"document_map"`
	HintMap     string         `json:"hint_map" yaml:"hint_map"`
	RetryConfig retries.Config `json:",inline" yaml:",inline"`
}

// NewMongoDBConfig returns a MongoDBConfig with default values.
func NewMongoDBConfig() MongoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return MongoDBConfig{
		MongoDB:      client.NewConfig(),
		Parts:        []int{},
		Operation:    "insert",
		RetryConfig:  rConf,
		WriteConcern: client.WriteConcern{},
	}
}
