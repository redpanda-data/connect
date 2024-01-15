package config

import (
	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/config/test"
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
	HTTP                   api.Config `yaml:"http"`
	stream.Config          `yaml:",inline"`
	manager.ResourceConfig `yaml:",inline"`
	Logger                 log.Config     `yaml:"logger"`
	Metrics                metrics.Config `yaml:"metrics"`
	Tracer                 tracer.Config  `yaml:"tracer"`
	SystemCloseDelay       string         `yaml:"shutdown_delay"`
	SystemCloseTimeout     string         `yaml:"shutdown_timeout"`
	Tests                  []any          `yaml:"tests"`

	rawSource any
}

func (t *Type) GetRawSource() any {
	return t.rawSource
}

var httpField = docs.FieldObject(fieldHTTP, "Configures the service-wide HTTP server.").WithChildren(api.Spec()...)

func observabilityFields() docs.FieldSpecs {
	defaultMetrics := "none"
	if _, exists := bundle.GlobalEnvironment.GetDocs("prometheus", docs.TypeMetrics); exists {
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
	fields = append(fields, test.ConfigSpec().Advanced())
	return fields
}

// SpecWithoutStream describes a stream config without the core stream fields.
func SpecWithoutStream() docs.FieldSpecs {
	fields := docs.FieldSpecs{httpField}
	fields = append(fields, manager.Spec()...)
	fields = append(fields, observabilityFields()...)
	fields = append(fields, test.ConfigSpec())
	return fields
}

func FromParsed(prov docs.Provider, pConf *docs.ParsedConfig, rawSource any) (conf Type, err error) {
	conf.rawSource = rawSource
	if conf.Config, err = stream.FromParsed(prov, pConf, nil); err != nil {
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
	if pConf.Contains(fieldTests) {
		var tmpTests []*docs.ParsedConfig
		if tmpTests, err = pConf.FieldAnyList(fieldTests); err != nil {
			return
		}
		for _, v := range tmpTests {
			t, _ := v.FieldAny()
			conf.Tests = append(conf.Tests, t)
		}
	}
	return
}
