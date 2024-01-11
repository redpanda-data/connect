package config

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

const (
	fieldHTTP               = "http"
	fieldLogger             = "logger"
	fieldMetrics            = "metrics"
	fieldTracer             = "tracer"
	fieldSystemCloseDelay   = "shutdown_delay"
	fieldSystemCloseTimeout = "shutdown_timeout"
	fieldTests              = "tests"
)

// Type is the Benthos service configuration struct.
type Type struct {
	HTTP                   api.Config `json:"http" yaml:"http"`
	stream.Config          `json:",inline" yaml:",inline"`
	manager.ResourceConfig `json:",inline" yaml:",inline"`
	Logger                 log.Config     `json:"logger" yaml:"logger"`
	Metrics                metrics.Config `json:"metrics" yaml:"metrics"`
	Tracer                 tracer.Config  `json:"tracer" yaml:"tracer"`
	SystemCloseDelay       string         `json:"shutdown_delay" yaml:"shutdown_delay"`
	SystemCloseTimeout     string         `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	Tests                  []any          `json:"tests,omitempty" yaml:"tests,omitempty"`
}

// New returns a new configuration with default values.
func New() Type {
	return Type{
		HTTP:               api.NewConfig(),
		Config:             stream.NewConfig(),
		ResourceConfig:     manager.NewResourceConfig(),
		Logger:             log.NewConfig(),
		Metrics:            metrics.NewConfig(),
		Tracer:             tracer.NewConfig(),
		SystemCloseDelay:   "",
		SystemCloseTimeout: "20s",
		Tests:              nil,
	}
}

// Sanitised returns a sanitised copy of the Benthos configuration, meaning
// fields of no consequence (unused inputs, outputs, processors etc) are
// excluded.
func (c Type) Sanitised() (any, error) {
	var node yaml.Node
	if err := node.Encode(c); err != nil {
		return nil, err
	}

	sanitConf := docs.NewSanitiseConfig()
	sanitConf.RemoveTypeField = true
	if err := Spec().SanitiseYAML(&node, sanitConf); err != nil {
		return nil, err
	}

	var g any
	if err := node.Decode(&g); err != nil {
		return nil, err
	}
	return g, nil
}

var httpField = docs.FieldObject(fieldHTTP, "Configures the service-wide HTTP server.").WithChildren(api.Spec()...)

func observabilityFields() docs.FieldSpecs {
	defaultMetrics := "none"
	if _, exists := docs.DeprecatedProvider.GetDocs("prometheus", docs.TypeMetrics); exists {
		defaultMetrics = "prometheus"
	}
	return docs.FieldSpecs{
		docs.FieldObject(fieldLogger, "Describes how operational logs should be emitted.").WithChildren(log.Spec()...),
		docs.FieldMetrics(fieldMetrics, "A mechanism for exporting metrics.").HasDefault(map[string]any{
			"mapping":      "",
			defaultMetrics: map[string]any{},
		}),
		docs.FieldTracer(fieldTracer, "A mechanism for exporting traces.").HasDefault(map[string]any{
			"none": map[string]any{},
		}),
		docs.FieldString(fieldSystemCloseDelay, "A period of time to wait for metrics and traces to be pulled or pushed from the process.").HasDefault("0s"),
		docs.FieldString(fieldSystemCloseTimeout, "The maximum period of time to wait for a clean shutdown. If this time is exceeded Benthos will forcefully close.").HasDefault("20s"),
	}
}

// Spec returns a docs.FieldSpec for an entire Benthos configuration.
func Spec() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, stream.Spec()...)
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields()...)
	fields = append(fields, tdocs.ConfigSpec())
	return fields
}

// SpecWithoutStream describes a stream config without the core stream fields.
func SpecWithoutStream() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields()...)
	fields = append(fields, tdocs.ConfigSpec())
	return fields
}

// FromYAML is for old style tests.
func FromYAML(confStr string) (Type, error) {
	node, err := docs.UnmarshalYAML([]byte(confStr))
	if err != nil {
		return Type{}, err
	}

	pConf, err := Spec().ParsedConfigFromAny(node)
	if err != nil {
		return Type{}, err
	}
	return FromParsed(docs.DeprecatedProvider, pConf)
}

func FromParsed(prov docs.Provider, pConf *docs.ParsedConfig) (conf Type, err error) {
	if conf.Config, err = stream.FromParsed(prov, pConf); err != nil {
		return
	}
	if conf.ResourceConfig, err = manager.FromParsed(prov, pConf); err != nil {
		return
	}
	err = noStreamFromParsed(prov, pConf, &conf)
	return
}

func noStreamFromParsed(prov docs.Provider, pConf *docs.ParsedConfig, conf *Type) (err error) {
	if pConf.Contains(fieldHTTP) {
		if conf.HTTP, err = api.FromParsed(pConf.Namespace(fieldHTTP)); err != nil {
			return
		}
	} else {
		conf.HTTP = api.NewConfig()
	}
	if pConf.Contains(fieldLogger) {
		if conf.Logger, err = log.FromParsed(pConf.Namespace(fieldLogger)); err != nil {
			return
		}
	} else {
		conf.Logger = log.NewConfig()
	}
	if ga, _ := pConf.FieldAny(fieldMetrics); ga != nil {
		if conf.Metrics, err = metrics.FromAny(prov, ga); err != nil {
			return
		}
	} else {
		conf.Metrics = metrics.NewConfig()
	}
	if ga, _ := pConf.FieldAny(fieldTracer); ga != nil {
		if conf.Tracer, err = tracer.FromAny(prov, ga); err != nil {
			return
		}
	} else {
		conf.Tracer = tracer.NewConfig()
	}
	if pConf.Contains(fieldSystemCloseDelay) {
		if conf.SystemCloseDelay, err = pConf.FieldString(fieldSystemCloseDelay); err != nil {
			return
		}
	}
	if pConf.Contains(fieldSystemCloseTimeout) {
		if conf.SystemCloseTimeout, err = pConf.FieldString(fieldSystemCloseTimeout); err != nil {
			return
		}
	}
	return
}
