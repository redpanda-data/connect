package cache

import (
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
)

// MongoDBConfig is a config struct for a mongo connection.
type MongoDBConfig struct {
	client.Config `json:",inline" yaml:",inline"`
	KeyField      string `json:"key_field" yaml:"key_field"`
	ValueField    string `json:"value_field" yaml:"value_field"`
}

// NewMongoDBConfig returns a MongoDBConfig with default values.
func NewMongoDBConfig() MongoDBConfig {
	return MongoDBConfig{
		Config:     client.NewConfig(),
		KeyField:   "",
		ValueField: "",
	}
}
