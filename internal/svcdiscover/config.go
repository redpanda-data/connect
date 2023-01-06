package svcdiscover

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	yaml "gopkg.in/yaml.v3"
)

type Config struct {
	Label   string      `json:"label" yaml:"label"`
	Type    string      `json:"type" yaml:"type"`
	Enabled bool        `json:"enabled" yaml:"enabled"`
	Plugin  any         `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Nacos   NacosConfig `json:"nacos" yaml:"nacos"`
}

func NewConfig() Config {
	return Config{
		Label:   "",
		Type:    "service_discover",
		Enabled: false,
		Plugin:  nil,
		Nacos:   NewNacosConfig(),
	}
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeProcessor, value); err != nil {
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
