package metrics

import (
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// InfluxDBConfig is config for the influx metrics type.
type InfluxDBConfig struct {
	URL string `json:"url" yaml:"url"`
	DB  string `json:"db" yaml:"db"`

	TLS              btls.Config     `json:"tls" yaml:"tls"`
	Interval         string          `json:"interval" yaml:"interval"`
	Password         string          `json:"password" yaml:"password"`
	PingInterval     string          `json:"ping_interval" yaml:"ping_interval"`
	Precision        string          `json:"precision" yaml:"precision"`
	Timeout          string          `json:"timeout" yaml:"timeout"`
	Username         string          `json:"username" yaml:"username"`
	RetentionPolicy  string          `json:"retention_policy" yaml:"retention_policy"`
	WriteConsistency string          `json:"write_consistency" yaml:"write_consistency"`
	Include          InfluxDBInclude `json:"include" yaml:"include"`

	Tags map[string]string `json:"tags" yaml:"tags"`
}

// InfluxDBInclude contains configuration parameters for optional metrics to
// include.
type InfluxDBInclude struct {
	Runtime string `json:"runtime" yaml:"runtime"`
	DebugGC string `json:"debug_gc" yaml:"debug_gc"`
}

// NewInfluxDBConfig creates an InfluxDBConfig struct with default values.
func NewInfluxDBConfig() InfluxDBConfig {
	return InfluxDBConfig{
		URL: "",
		DB:  "",
		TLS: btls.NewConfig(),

		Precision:    "s",
		Interval:     "1m",
		PingInterval: "20s",
		Timeout:      "5s",
	}
}
