package input

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component/input"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var (
	// ErrBrokerNoInputs is returned when creating a broker with zero inputs.
	ErrBrokerNoInputs = errors.New("attempting to create broker input type with no inputs")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBroker] = TypeSpec{
		constructor: NewBroker,
		Summary: `
Allows you to combine multiple inputs into a single stream of data, where each input will be read in parallel.`,
		Description: `
A broker type is configured with its own list of input configurations and a field to specify how many copies of the list of inputs should be created.

Adding more input types allows you to combine streams from multiple sources into one. For example, reading from both RabbitMQ and Kafka:

` + "```yaml" + `
input:
  broker:
    copies: 1
    inputs:
      - amqp_0_9:
          urls:
            - amqp://guest:guest@localhost:5672/
          consumer_tag: benthos-consumer
          queue: benthos-queue

        # Optional list of input specific processing steps
        processors:
          - bloblang: |
              root.message = this
              root.meta.link_count = this.links.length()
              root.user.age = this.user.age.number()

      - kafka:
          addresses:
            - localhost:9092
          client_id: benthos_kafka_input
          consumer_group: benthos_consumer_group
          topics: [ benthos_stream:0 ]
` + "```" + `

If the number of copies is greater than zero the list will be copied that number
of times. For example, if your inputs were of type foo and bar, with 'copies'
set to '2', you would end up with two 'foo' inputs and two 'bar' inputs.

### Batching

It's possible to configure a [batch policy](/docs/configuration/batching#batch-policy)
with a broker using the ` + "`batching`" + ` fields. When doing this the feeds
from all child inputs are combined. Some inputs do not support broker based
batching and specify this in their documentation.

### Processors

It is possible to configure [processors](/docs/components/processors/about) at
the broker level, where they will be applied to _all_ child inputs, as well as
on the individual child inputs. If you have processors at both the broker level
_and_ on child inputs then the broker processors will be applied _after_ the
child nodes processors.`,
		Categories: []Category{
			CategoryUtility,
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced("copies", "Whatever is specified within `inputs` will be created this many times."),
			docs.FieldCommon("inputs", "A list of inputs to create.").Array().HasType(docs.FieldTypeInput),
			batch.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// BrokerConfig contains configuration fields for the Broker input type.
type BrokerConfig struct {
	Copies   int                `json:"copies" yaml:"copies"`
	Inputs   brokerInputList    `json:"inputs" yaml:"inputs"`
	Batching batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Copies:   1,
		Inputs:   brokerInputList{},
		Batching: batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

type brokerInputList []Config

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerInputList) UnmarshalJSON(bytes []byte) error {
	genericInputs := []interface{}{}
	if err := json.Unmarshal(bytes, &genericInputs); err != nil {
		return err
	}

	inputConfs, err := parseInputConfsWithDefaults(genericInputs)
	if err != nil {
		return err
	}

	*b = inputConfs
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerInputList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	genericInputs := []interface{}{}
	if err := unmarshal(&genericInputs); err != nil {
		return err
	}

	inputConfs, err := parseInputConfsWithDefaults(genericInputs)
	if err != nil {
		return err
	}

	*b = inputConfs
	return nil
}

//------------------------------------------------------------------------------

// parseInputConfsWithDefaults takes a slice of generic input configs and
// returns a slice of input configs with default values in place of omitted
// values. This is necessary because when unmarshalling config files using
// structs you can pre-populate non-reference type struct fields with default
// values, but array objects will lose those defaults.
//
// In order to ensure that omitted values are set to default we initially parse
// the array as interface{} types and then individually apply the defaults by
// marshalling and unmarshalling. The correct way to do this would be to use
// json.RawMessage, but our config files can contain a range of different
// formats that we do not know at this stage, therefore we use the more hacky
// method as performance is not an issue at this stage.
//
// TODO: V4 Pretty sure we can get rid of all of this.
func parseInputConfsWithDefaults(rawInputs []interface{}) ([]Config, error) {
	inputConfs := []Config{}

	// NOTE: Use yaml here as it supports more types than JSON
	// (map[interface{}]interface{}).
	for _, boxedConfig := range rawInputs {
		newConfigs := make([]Config, 1)

		for _, conf := range newConfigs {
			rawBytes, err := yaml.Marshal(boxedConfig)
			if err != nil {
				return nil, err
			}
			if err := yaml.Unmarshal(rawBytes, &conf); err != nil {
				return nil, err
			}
			inputConfs = append(inputConfs, conf)
		}
	}

	return inputConfs, nil
}

//------------------------------------------------------------------------------

// NewBroker creates a new Broker input type.
func NewBroker(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...iprocessor.PipelineConstructorFunc,
) (input.Streamed, error) {
	pipelines = AppendProcessorsFromConfig(conf, mgr, log, stats, pipelines...)

	lInputs := len(conf.Broker.Inputs) * conf.Broker.Copies

	if lInputs <= 0 {
		return nil, ErrBrokerNoInputs
	}

	var err error
	var b input.Streamed
	if lInputs == 1 {
		if b, err = New(conf.Broker.Inputs[0], mgr, log, stats, pipelines...); err != nil {
			return nil, err
		}
	} else {
		inputs := make([]input.Streamed, lInputs)

		for j := 0; j < conf.Broker.Copies; j++ {
			for i, iConf := range conf.Broker.Inputs {
				iMgr, iLog, iStats := interop.LabelChild(fmt.Sprintf("broker.inputs.%v", i), mgr, log, stats)
				iStats = metrics.Combine(stats, iStats)
				inputs[len(conf.Broker.Inputs)*j+i], err = New(iConf, iMgr, iLog, iStats, pipelines...)
				if err != nil {
					return nil, fmt.Errorf("failed to create input '%v' type '%v': %v", i, iConf.Type, err)
				}
			}
		}

		if b, err = broker.NewFanIn(inputs, stats); err != nil {
			return nil, err
		}
	}

	if conf.Broker.Batching.IsNoop() {
		return b, nil
	}

	bMgr, bLog, bStats := interop.LabelChild("batching", mgr, log, stats)
	policy, err := batch.NewPolicy(conf.Broker.Batching, bMgr, bLog, bStats)
	if err != nil {
		return nil, fmt.Errorf("failed to construct batch policy: %v", err)
	}

	return NewBatcher(policy, b, log, stats), nil
}

//------------------------------------------------------------------------------
