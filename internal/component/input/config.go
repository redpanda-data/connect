package input

import (
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all input types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label        string             `json:"label" yaml:"label"`
	Type         string             `json:"type" yaml:"type"`
	Broker       BrokerConfig       `json:"broker" yaml:"broker"`
	Dynamic      DynamicConfig      `json:"dynamic" yaml:"dynamic"`
	Generate     GenerateConfig     `json:"generate" yaml:"generate"`
	HTTPServer   HTTPServerConfig   `json:"http_server" yaml:"http_server"`
	Inproc       InprocConfig       `json:"inproc" yaml:"inproc"`
	Kafka        KafkaConfig        `json:"kafka" yaml:"kafka"`
	MQTT         MQTTConfig         `json:"mqtt" yaml:"mqtt"`
	Nanomsg      NanomsgConfig      `json:"nanomsg" yaml:"nanomsg"`
	NSQ          NSQConfig          `json:"nsq" yaml:"nsq"`
	Plugin       any                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ReadUntil    ReadUntilConfig    `json:"read_until" yaml:"read_until"`
	Resource     string             `json:"resource" yaml:"resource"`
	Sequence     SequenceConfig     `json:"sequence" yaml:"sequence"`
	SFTP         SFTPConfig         `json:"sftp" yaml:"sftp"`
	Socket       SocketConfig       `json:"socket" yaml:"socket"`
	SocketServer SocketServerConfig `json:"socket_server" yaml:"socket_server"`
	STDIN        STDINConfig        `json:"stdin" yaml:"stdin"`
	Subprocess   SubprocessConfig   `json:"subprocess" yaml:"subprocess"`
	Processors   []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
func NewConfig() Config {
	return Config{
		Label:        "",
		Type:         "stdin",
		Broker:       NewBrokerConfig(),
		Dynamic:      NewDynamicConfig(),
		Generate:     NewGenerateConfig(),
		HTTPServer:   NewHTTPServerConfig(),
		Inproc:       NewInprocConfig(),
		Kafka:        NewKafkaConfig(),
		MQTT:         NewMQTTConfig(),
		Nanomsg:      NewNanomsgConfig(),
		NSQ:          NewNSQConfig(),
		Plugin:       nil,
		ReadUntil:    NewReadUntilConfig(),
		Resource:     "",
		Sequence:     NewSequenceConfig(),
		SFTP:         NewSFTPConfig(),
		Socket:       NewSocketConfig(),
		SocketServer: NewSocketServerConfig(),
		STDIN:        NewSTDINConfig(),
		Subprocess:   NewSubprocessConfig(),
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
		return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeInput, value); err != nil {
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
