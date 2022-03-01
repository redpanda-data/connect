package input

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/batch/policy"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/old/broker"
	"github.com/Jeffail/benthos/v3/internal/old/metrics"
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
			policy.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// BrokerConfig contains configuration fields for the Broker input type.
type BrokerConfig struct {
	Copies   int           `json:"copies" yaml:"copies"`
	Inputs   []Config      `json:"inputs" yaml:"inputs"`
	Batching policy.Config `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Copies:   1,
		Inputs:   []Config{},
		Batching: policy.NewConfig(),
	}
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
				iMgr := mgr.IntoPath("broker", "inputs", strconv.Itoa(i))
				inputs[len(conf.Broker.Inputs)*j+i], err = New(iConf, iMgr, iMgr.Logger(), iMgr.Metrics(), pipelines...)
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

	bMgr := mgr.IntoPath("broker", "batching")
	policy, err := policy.New(conf.Broker.Batching, bMgr)
	if err != nil {
		return nil, fmt.Errorf("failed to construct batch policy: %v", err)
	}

	return NewBatcher(policy, b, log, stats), nil
}

//------------------------------------------------------------------------------
