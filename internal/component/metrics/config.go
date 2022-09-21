package metrics

import (
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
	Plugin        any              `json:"plugin,omitempty" yaml:"plugin,omitempty"`
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
		Plugin:        nil,
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
		return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeMetrics, value); err != nil {
		return docs.NewLintError(value.Line, docs.LintComponentMissing, err.Error())
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}
