package output

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all output types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label        string             `json:"label" yaml:"label"`
	Type         string             `json:"type" yaml:"type"`
	Broker       BrokerConfig       `json:"broker" yaml:"broker"`
	Cache        CacheConfig        `json:"cache" yaml:"cache"`
	Cassandra    CassandraConfig    `json:"cassandra" yaml:"cassandra"`
	Drop         DropConfig         `json:"drop" yaml:"drop"`
	DropOn       DropOnConfig       `json:"drop_on" yaml:"drop_on"`
	Dynamic      DynamicConfig      `json:"dynamic" yaml:"dynamic"`
	Fallback     TryConfig          `json:"fallback" yaml:"fallback"`
	Inproc       string             `json:"inproc" yaml:"inproc"`
	NSQ          NSQConfig          `json:"nsq" yaml:"nsq"`
	Plugin       any                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Reject       string             `json:"reject" yaml:"reject"`
	Resource     string             `json:"resource" yaml:"resource"`
	Retry        RetryConfig        `json:"retry" yaml:"retry"`
	SFTP         SFTPConfig         `json:"sftp" yaml:"sftp"`
	STDOUT       STDOUTConfig       `json:"stdout" yaml:"stdout"`
	Subprocess   SubprocessConfig   `json:"subprocess" yaml:"subprocess"`
	Switch       SwitchConfig       `json:"switch" yaml:"switch"`
	SyncResponse struct{}           `json:"sync_response" yaml:"sync_response"`
	Socket       SocketConfig       `json:"socket" yaml:"socket"`
	Processors   []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
func NewConfig() Config {
	return Config{
		Label:        "",
		Type:         "stdout",
		Broker:       NewBrokerConfig(),
		Cache:        NewCacheConfig(),
		Cassandra:    NewCassandraConfig(),
		Drop:         NewDropConfig(),
		DropOn:       NewDropOnConfig(),
		Dynamic:      NewDynamicConfig(),
		Fallback:     NewTryConfig(),
		Inproc:       "",
		NSQ:          NewNSQConfig(),
		Plugin:       nil,
		Reject:       "",
		Resource:     "",
		Retry:        NewRetryConfig(),
		SFTP:         NewSFTPConfig(),
		STDOUT:       NewSTDOUTConfig(),
		Subprocess:   NewSubprocessConfig(),
		Switch:       NewSwitchConfig(),
		SyncResponse: struct{}{},
		Socket:       NewSocketConfig(),
		Processors:   []processor.Config{},
	}
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return docs.NewLintError(value.Line, docs.LintFailedRead, err)
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeOutput, value); err != nil {
		return docs.NewLintError(value.Line, docs.LintComponentMissing, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return docs.NewLintError(value.Line, docs.LintFailedRead, err)
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}
