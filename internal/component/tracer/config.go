package tracer

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

func init() {
	// TODO: I'm so confused, these APIs are a nightmare.
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// Config is the all encompassing configuration struct for all tracer types.
type Config struct {
	Type       string           `json:"type" yaml:"type"`
	Jaeger     JaegerConfig     `json:"jaeger" yaml:"jaeger"`
	CloudTrace CloudTraceConfig `json:"gcp_cloudtrace" yaml:"gcp_cloudtrace"`
	None       struct{}         `json:"none" yaml:"none"`
	Plugin     any              `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:       "none",
		Jaeger:     NewJaegerConfig(),
		CloudTrace: NewCloudTraceConfig(),
		None:       struct{}{},
		Plugin:     nil,
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeTracer, value); err != nil {
		return docs.NewLintError(value.Line, docs.LintComponentNotFound, err.Error())
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
