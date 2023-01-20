package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// OpenSearchAuthConfig contains basic authentication fields.
type OpenSearchAuthConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// OpenSearchConfig contains configuration fields for the OpenSearch
// output type.
type OpenSearchConfig struct {
	URLs            []string             `json:"urls" yaml:"urls"`
	ID              string               `json:"id" yaml:"id"`
	Action          string               `json:"action" yaml:"action"`
	Index           string               `json:"index" yaml:"index"`
	Pipeline        string               `json:"pipeline" yaml:"pipeline"`
	Routing         string               `json:"routing" yaml:"routing"`
	Type            string               `json:"type" yaml:"type"`
	FlushInterval   string               `json:"flush_interval" yaml:"flush_interval"`
	FlushBytes      string               `json:"flush_bytes" yaml:"flush_bytes"`
	Timeout         string               `json:"timeout" yaml:"timeout"`
	TLS             btls.Config          `json:"tls" yaml:"tls"`
	Auth            OpenSearchAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	AWS             OptionalAWSConfig    `json:"aws" yaml:"aws"`
	GzipCompression bool                 `json:"gzip_compression" yaml:"gzip_compression"`
	MaxInFlight     int                  `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config  `json:",inline" yaml:",inline"`
	Batching        batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewOpenSearchConfig creates a new OpenSearchConfig with default values.
func NewOpenSearchConfig() OpenSearchConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return OpenSearchConfig{
		URLs:          []string{},
		Action:        "index",
		ID:            `${!count("elastic_ids")}-${!timestamp_unix()}`,
		Index:         "",
		Pipeline:      "",
		Type:          "",
		Routing:       "",
		Timeout:       "5s",
		FlushInterval: "30s",
		FlushBytes:    "5242880",
		TLS:           btls.NewConfig(),
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
