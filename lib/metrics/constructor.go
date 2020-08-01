package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

// Errors for the metrics package.
var (
	ErrInvalidMetricOutputType = errors.New("invalid metrics output type")
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each metric output
// type.
type TypeSpec struct {
	constructor        func(conf Config, opts ...func(Type)) (Type, error)
	sanitiseConfigFunc func(conf Config) (interface{}, error)

	Summary     string
	Description string
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
	Deprecated  bool
}

// Constructors is a map of all metrics types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each metric type.
const (
	TypeBlackList  = "blacklist"
	TypeCloudWatch = "cloudwatch"
	TypeHTTPServer = "http_server"
	TypePrometheus = "prometheus"
	TypeRename     = "rename"
	TypeStatsd     = "statsd"
	TypeStdout     = "stdout"
	TypeWhiteList  = "whitelist"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all metric output
// types.
type Config struct {
	Type       string           `json:"type" yaml:"type"`
	Blacklist  BlacklistConfig  `json:"blacklist" yaml:"blacklist"`
	CloudWatch CloudWatchConfig `json:"cloudwatch" yaml:"cloudwatch"`
	HTTP       HTTPConfig       `json:"http_server" yaml:"http_server"`
	Prometheus PrometheusConfig `json:"prometheus" yaml:"prometheus"`
	Rename     RenameConfig     `json:"rename" yaml:"rename"`
	Statsd     StatsdConfig     `json:"statsd" yaml:"statsd"`
	Stdout     StdoutConfig     `json:"stdout" yaml:"stdout"`
	Whitelist  WhitelistConfig  `json:"whitelist" yaml:"whitelist"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:       "http_server",
		Blacklist:  NewBlacklistConfig(),
		CloudWatch: NewCloudWatchConfig(),
		HTTP:       NewHTTPConfig(),
		Prometheus: NewPrometheusConfig(),
		Rename:     NewRenameConfig(),
		Statsd:     NewStatsdConfig(),
		Stdout:     NewStdoutConfig(),
		Whitelist:  NewWhitelistConfig(),
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := config.Sanitised{}

	t := conf.Type
	outputMap["type"] = t
	if sfunc := Constructors[t].sanitiseConfigFunc; sfunc != nil {
		if outputMap[t], err = sfunc(conf); err != nil {
			return nil, err
		}
	} else {
		outputMap[t] = hashMap[t]
	}

	def := Constructors[t]

	if removeDeprecated {
		if m, ok := outputMap[t].(map[string]interface{}); ok {
			for _, spec := range def.FieldSpecs {
				if spec.Deprecated {
					delete(m, spec.Name)
				}
			}
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalJSON(bytes []byte) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*conf = Config(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	var raw interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("unable to infer type, multiple candidates '%v' and '%v'", inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("unable to infer type, candidates were: %v", typeCandidates)
		}
		aliased.Type = inferredType
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

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-metrics`" + `

A metrics type represents a destination for Benthos metrics to be aggregated
such as Statsd, Prometheus, or for debugging purposes an HTTP endpoint that
exposes a JSON object of metrics.

A metrics config section looks like this:

` + "``` yaml" + `
metrics:
  statsd:
    prefix: foo
    address: localhost:8125
    flush_period: 100ms
` + "```" + `

Benthos exposes lots of metrics and their paths will depend on your pipeline
configuration. However, there are some critical metrics that will always be
present that are outlined in [this document](#paths).`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Metric Target Types\n")
	buf.WriteString(strings.Repeat("=", 19))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = config.MarshalYAML(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(Constructors[name].Description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	return buf.String()
}

// New creates a metric output type based on a configuration.
func New(conf Config, opts ...func(Type)) (Type, error) {
	if conf.Type == "none" {
		return DudType{}, nil
	}
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, opts...)
	}
	return nil, ErrInvalidMetricOutputType
}

//------------------------------------------------------------------------------
