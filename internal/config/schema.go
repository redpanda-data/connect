package config

import (
	"github.com/mitchellh/mapstructure"

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

// Clone a config, creating a new copy that can be mutated in isolation.
func (t *Type) Clone() (Type, error) {
	var outConf Type
	if err := mapstructure.Decode(t, &outConf); err != nil {
		return Type{}, err
	}
	return outConf, nil
}

var httpField = docs.FieldObject(fieldHTTP, "Configures the service-wide HTTP server.").WithChildren(api.Spec()...)

var observabilityFields = docs.FieldSpecs{
	docs.FieldObject(fieldLogger, "Describes how operational logs should be emitted.").WithChildren(log.Spec()...),
	docs.FieldMetrics(fieldMetrics, "A mechanism for exporting metrics.").Optional(),
	docs.FieldTracer(fieldTracer, "A mechanism for exporting traces.").Optional(),
	docs.FieldString(fieldSystemCloseDelay, "A period of time to wait for metrics and traces to be pulled or pushed from the process.").HasDefault("0s"),
	docs.FieldString(fieldSystemCloseTimeout, "The maximum period of time to wait for a clean shutdown. If this time is exceeded Benthos will forcefully close.").HasDefault("20s"),
}

// Spec returns a docs.FieldSpec for an entire Benthos configuration.
func Spec() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, stream.Spec()...)
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields...)
	fields = append(fields, tdocs.ConfigSpec())
	return fields
}

// SpecWithoutStream describes a stream config without the core stream fields.
func SpecWithoutStream() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields...)
	fields = append(fields, tdocs.ConfigSpec())
	return fields
}

func (t *Type) FromAny(prov docs.Provider, v any) (err error) {
	if err = t.Config.FromAny(prov, v); err != nil {
		return
	}
	if err = t.ResourceConfig.FromAny(prov, v); err != nil {
		return
	}
	var pConf *docs.ParsedConfig
	if pConf, err = Spec().ParsedConfigFromAny(v); err != nil {
		return
	}
	if pConf.Contains(fieldHTTP) {
		if t.HTTP, err = api.FromParsed(pConf.Namespace(fieldHTTP)); err != nil {
			return
		}
	}
	if pConf.Contains(fieldLogger) {
		if t.Logger, err = log.FromParsed(pConf.Namespace(fieldLogger)); err != nil {
			return
		}
	}
	if ga, _ := pConf.FieldAny(fieldMetrics); ga != nil {
		if t.Metrics, err = metrics.FromAny(prov, ga); err != nil {
			return
		}
	} else {
		t.Metrics = metrics.NewConfig()
	}
	if ga, _ := pConf.FieldAny(fieldTracer); ga != nil {
		if t.Tracer, err = tracer.FromAny(prov, ga); err != nil {
			return
		}
	} else {
		t.Tracer = tracer.NewConfig()
	}
	if pConf.Contains(fieldSystemCloseDelay) {
		if t.SystemCloseDelay, err = pConf.FieldString(fieldSystemCloseDelay); err != nil {
			return
		}
	}
	if pConf.Contains(fieldSystemCloseTimeout) {
		if t.SystemCloseTimeout, err = pConf.FieldString(fieldSystemCloseTimeout); err != nil {
			return
		}
	}
	return
}
