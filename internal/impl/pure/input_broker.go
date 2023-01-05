package pure

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// ErrBrokerNoInputs is returned when creating a broker with zero inputs.
var ErrBrokerNoInputs = errors.New("attempting to create broker input type with no inputs")

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newBrokerInput), docs.ComponentSpec{
		Name: "broker",
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
          - mapping: |
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
		Categories: []string{
			"Utility",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("copies", "Whatever is specified within `inputs` will be created this many times.").Advanced().HasDefault(1),
			docs.FieldInput("inputs", "A list of inputs to create.").Array().HasDefault([]any{}),
			policy.FieldSpec(),
		),
	})
	if err != nil {
		panic(err)
	}
}

func newBrokerInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	lInputs := len(conf.Broker.Inputs) * conf.Broker.Copies
	if lInputs <= 0 {
		return nil, ErrBrokerNoInputs
	}

	var err error
	var b input.Streamed
	if lInputs == 1 {
		if b, err = mgr.NewInput(conf.Broker.Inputs[0]); err != nil {
			return nil, err
		}
	} else {
		inputs := make([]input.Streamed, lInputs)

		for j := 0; j < conf.Broker.Copies; j++ {
			for i, iConf := range conf.Broker.Inputs {
				iMgr := mgr.IntoPath("broker", "inputs", strconv.Itoa(i))
				inputs[len(conf.Broker.Inputs)*j+i], err = iMgr.NewInput(iConf)
				if err != nil {
					return nil, err
				}
			}
		}

		if b, err = newFanInInputBroker(inputs); err != nil {
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

	return batcher.New(policy, b, mgr.Logger()), nil
}
