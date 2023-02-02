package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// OptionalAWSConfig contains config fields for AWS authentication with an
// enable flag.
type OptionalAWSConfig struct {
	Enabled     bool `json:"enabled" yaml:"enabled"`
	sess.Config `json:",inline" yaml:",inline"`
}

// ElasticsearchAuthConfig contains basic authentication fields.
type ElasticsearchAuthConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// ElasticsearchConfig contains configuration fields for the Elasticsearch
// output type.
type ElasticsearchConfig struct {
	URLs            []string                `json:"urls" yaml:"urls"`
	Sniff           bool                    `json:"sniff" yaml:"sniff"`
	Healthcheck     bool                    `json:"healthcheck" yaml:"healthcheck"`
	ID              string                  `json:"id" yaml:"id"`
	Action          string                  `json:"action" yaml:"action"`
	Index           string                  `json:"index" yaml:"index"`
	Pipeline        string                  `json:"pipeline" yaml:"pipeline"`
	Routing         string                  `json:"routing" yaml:"routing"`
	Type            string                  `json:"type" yaml:"type"`
	Timeout         string                  `json:"timeout" yaml:"timeout"`
	TLS             btls.Config             `json:"tls" yaml:"tls"`
	Auth            ElasticsearchAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	AWS             OptionalAWSConfig       `json:"aws" yaml:"aws"`
	GzipCompression bool                    `json:"gzip_compression" yaml:"gzip_compression"`
	MaxInFlight     int                     `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config  `json:",inline" yaml:",inline"`
	Batching        batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewElasticsearchConfig creates a new ElasticsearchConfig with default values.
func NewElasticsearchConfig() ElasticsearchConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return ElasticsearchConfig{
		URLs:        []string{},
		Sniff:       true,
		Healthcheck: true,
		Action:      "index",
		ID:          `${!count("elastic_ids")}-${!timestamp_unix()}`,
		Index:       "",
		Pipeline:    "",
		Type:        "",
		Routing:     "",
		Timeout:     "5s",
		TLS:         btls.NewConfig(),
		AWS: OptionalAWSConfig{
			Enabled: false,
			Config:  sess.NewConfig(),
		},
		GzipCompression: false,
		MaxInFlight:     64,
		Config:          rConf,
		Batching:        batchconfig.NewConfig(),
	}
}
