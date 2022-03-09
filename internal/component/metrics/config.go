package metrics

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all metric output
// types.
type Config struct {
	Type          string           `json:"type" yaml:"type"`
	Mapping       string           `json:"mapping" yaml:"mapping"`
	AWSCloudWatch CloudWatchConfig `json:"aws_cloudwatch" yaml:"aws_cloudwatch"`
	JSONAPI       JSONAPIConfig    `json:"json_api" yaml:"json_api"`
	InfluxDB      InfluxDBConfig   `json:"influxdb" yaml:"influxdb"`
	None          struct{}         `json:"none" yaml:"none"`
	Prometheus    PrometheusConfig `json:"prometheus" yaml:"prometheus"`
	Statsd        StatsdConfig     `json:"statsd" yaml:"statsd"`
	Logger        LoggerConfig     `json:"logger" yaml:"logger"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          docs.DefaultTypeOf(docs.TypeMetrics),
		Mapping:       "",
		AWSCloudWatch: NewCloudWatchConfig(),
		JSONAPI:       NewJSONAPIConfig(),
		InfluxDB:      NewInfluxDBConfig(),
		None:          struct{}{},
		Prometheus:    NewPrometheusConfig(),
		Statsd:        NewStatsdConfig(),
		Logger:        NewLoggerConfig(),
	}
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	if aliased.Type, _, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeMetrics, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	*conf = Config(aliased)
	return nil
}
