package metrics

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"gopkg.in/yaml.v3"
)

// Errors for the metrics package.
var (
	ErrInvalidMetricOutputType = errors.New("invalid metrics output type")
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each metric output
// type.
type TypeSpec struct {
	constructor func(conf Config, log log.Modular) (Type, error)

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Footnotes   string
	config      docs.FieldSpec
	FieldSpecs  docs.FieldSpecs
}

// ConstructorFunc is a func signature able to construct a metrics output.
type ConstructorFunc func(Config, log.Modular) (Type, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.config
		if len(v.FieldSpecs) > 0 {
			conf = docs.FieldComponent().WithChildren(v.FieldSpecs.DefaultAndTypeFrom(inferred[k])...)
		} else {
			conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
		}
		spec := docs.ComponentSpec{
			Type:        docs.TypeMetrics,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Status:      v.Status,
			Version:     v.Version,
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all metrics types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each metric type.
const (
	TypeAWSCloudWatch = "aws_cloudwatch"
	TypeInfluxDB      = "influxdb"
	TypeNone          = "none"
	TypePrometheus    = "prometheus"
	TypeStatsd        = "statsd"
)

//------------------------------------------------------------------------------

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
		Type:          "prometheus",
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

//------------------------------------------------------------------------------

// New creates a metric output type based on a configuration.
func New(conf Config, log log.Modular) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, log)
	}
	return nil, ErrInvalidMetricOutputType
}
