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
	constructor func(conf Config, opts ...func(Type)) (Type, error)

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Footnotes   string
	config      docs.FieldSpec
	FieldSpecs  docs.FieldSpecs
}

// ConstructorFunc is a func signature able to construct a metrics output.
type ConstructorFunc func(Config, ...func(Type)) (Type, error)

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
	TypeHTTPServer    = "http_server"
	TypeInfluxDB      = "influxdb"
	TypeNone          = "none"
	TypePrometheus    = "prometheus"
	TypeStatsd        = "statsd"
	TypeStdout        = "stdout"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all metric output
// types.
type Config struct {
	Type          string           `json:"type" yaml:"type"`
	AWSCloudWatch CloudWatchConfig `json:"aws_cloudwatch" yaml:"aws_cloudwatch"`
	HTTP          HTTPConfig       `json:"http_server" yaml:"http_server"`
	InfluxDB      InfluxDBConfig   `json:"influxdb" yaml:"influxdb"`
	None          struct{}         `json:"none" yaml:"none"`
	Prometheus    PrometheusConfig `json:"prometheus" yaml:"prometheus"`
	Statsd        StatsdConfig     `json:"statsd" yaml:"statsd"`
	Stdout        StdoutConfig     `json:"stdout" yaml:"stdout"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          "http_server",
		AWSCloudWatch: NewCloudWatchConfig(),
		HTTP:          NewHTTPConfig(),
		InfluxDB:      NewInfluxDBConfig(),
		None:          struct{}{},
		Prometheus:    NewPrometheusConfig(),
		Statsd:        NewStatsdConfig(),
		Stdout:        NewStdoutConfig(),
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

	if aliased.Type, _, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeMetrics, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// OptSetLogger sets the logging output to be used by the metrics clients.
func OptSetLogger(log log.Modular) func(Type) {
	return func(t Type) {
		t.SetLogger(log)
	}
}

// New creates a metric output type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidMetricOutputType
}
