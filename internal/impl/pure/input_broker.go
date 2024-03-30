package pure

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/public/service"
)

// ErrBrokerNoInputs is returned when creating a broker with zero inputs.
var ErrBrokerNoInputs = errors.New("attempting to create broker input type with no inputs")

const (
	ibFieldCopies   = "copies"
	ibFieldInputs   = "inputs"
	ibFieldBatching = "batching"
)

func brokerInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Allows you to combine multiple inputs into a single stream of data, where each input will be read in parallel.").
		Description(`
A broker type is configured with its own list of input configurations and a field to specify how many copies of the list of inputs should be created.

Adding more input types allows you to combine streams from multiple sources into one. For example, reading from both RabbitMQ and Kafka:

`+"```yaml"+`
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
`+"```"+`

If the number of copies is greater than zero the list will be copied that number of times. For example, if your inputs were of type foo and bar, with 'copies' set to '2', you would end up with two 'foo' inputs and two 'bar' inputs.

### Batching

It's possible to configure a [batch policy](/docs/configuration/batching#batch-policy) with a broker using the `+"`batching`"+` fields. When doing this the feeds from all child inputs are combined. Some inputs do not support broker based batching and specify this in their documentation.

### Processors

It is possible to configure [processors](/docs/components/processors/about) at the broker level, where they will be applied to _all_ child inputs, as well as on the individual child inputs. If you have processors at both the broker level _and_ on child inputs then the broker processors will be applied _after_ the child nodes processors.`).
		Fields(
			service.NewIntField(ibFieldCopies).
				Description("Whatever is specified within `inputs` will be created this many times.").
				Advanced().
				Default(1),
			service.NewInputListField(ibFieldInputs).
				Description("A list of inputs to create."),
			service.NewBatchPolicyField("batching"),
		)
}

func init() {
	err := service.RegisterBatchInput("broker", brokerInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newBrokerInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

func newBrokerInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (input.Streamed, error) {
	copies, err := conf.FieldInt(ibFieldCopies)
	if err != nil {
		return nil, err
	}

	children, err := conf.FieldInputList(ibFieldInputs)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, ErrBrokerNoInputs
	}

	var b input.Streamed
	if len(children) == 1 && copies == 1 {
		b = interop.UnwrapOwnedInput(children[0])
	} else {
		var inputs []input.Streamed
		for _, v := range children {
			inputs = append(inputs, interop.UnwrapOwnedInput(v))
		}
		for j := 1; j < copies; j++ {
			extraChildren, err := conf.FieldInputList(ibFieldInputs)
			if err != nil {
				return nil, err
			}
			for _, v := range extraChildren {
				inputs = append(inputs, interop.UnwrapOwnedInput(v))
			}
		}
		if b, err = newFanInInputBroker(inputs); err != nil {
			return nil, err
		}
	}

	batcherPol, err := conf.FieldBatchPolicy(ibFieldBatching)
	if err != nil {
		return nil, err
	}

	if batcherPol.IsNoop() {
		return b, nil
	}

	pubBatcher, err := batcherPol.NewBatcher(mgr)
	if err != nil {
		return nil, err
	}

	iBatcher := interop.UnwrapBatcher(pubBatcher)
	return batcher.New(iBatcher, b, interop.UnwrapManagement(mgr).Logger()), nil
}
