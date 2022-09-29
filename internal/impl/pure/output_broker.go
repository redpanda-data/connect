package pure

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// ErrBrokerNoOutputs is returned when creating a Broker type with zero
// outputs.
var ErrBrokerNoOutputs = errors.New("attempting to create broker output type with no outputs")

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newBroker), docs.ComponentSpec{
		Name: "broker",
		Summary: `
Allows you to route messages to multiple child outputs using a range of
brokering [patterns](#patterns).`,
		Description: `
[Processors](/docs/components/processors/about) can be listed to apply across
individual outputs or all outputs:

` + "```yaml" + `
output:
  broker:
    pattern: fan_out
    outputs:
      - resource: foo
      - resource: bar
        # Processors only applied to messages sent to bar.
        processors:
          - resource: bar_processor

  # Processors applied to messages sent to all brokered outputs.
  processors:
    - resource: general_processor
` + "```" + ``,
		Footnotes: `
## Patterns

The broker pattern determines the way in which messages are allocated and can be
chosen from the following:

### ` + "`fan_out`" + `

With the fan out pattern all outputs will be sent every message that passes
through Benthos in parallel.

If an output applies back pressure it will block all subsequent messages, and if
an output fails to send a message it will be retried continuously until
completion or service shut down.

Sometimes it is useful to disable the back pressure or retries of certain fan
out outputs and instead drop messages that have failed or were blocked. In this
case you can wrap outputs with a ` + "[`drop_on` output](/docs/components/outputs/drop_on)" + `.

### ` + "`fan_out_sequential`" + `

Similar to the fan out pattern except outputs are written to sequentially,
meaning an output is only written to once the preceding output has confirmed
receipt of the same message.

### ` + "`round_robin`" + `

With the round robin pattern each message will be assigned a single output
following their order. If an output applies back pressure it will block all
subsequent messages. If an output fails to send a message then the message will
be re-attempted with the next input, and so on.

### ` + "`greedy`" + `

The greedy pattern results in higher output throughput at the cost of
potentially disproportionate message allocations to those outputs. Each message
is sent to a single output, which is determined by allowing outputs to claim
messages as soon as they are able to process them. This results in certain
faster outputs potentially processing more messages at the cost of slower
outputs.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("copies", "The number of copies of each configured output to spawn.").Advanced().HasDefault(1),
			docs.FieldString("pattern", "The brokering pattern to use.").HasOptions(
				"fan_out", "fan_out_sequential", "round_robin", "greedy",
			).HasDefault("fan_out"),
			docs.FieldOutput("outputs", "A list of child outputs to broker.").Array().HasDefault([]any{}),
			policy.FieldSpec(),
		),
		Categories: []string{
			"Utility",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newBroker(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	outputConfs := conf.Broker.Outputs
	lOutputs := len(outputConfs) * conf.Broker.Copies

	if lOutputs <= 0 {
		return nil, ErrBrokerNoOutputs
	}
	if lOutputs == 1 {
		b, err := mgr.NewOutput(outputConfs[0])
		if err != nil {
			return nil, err
		}
		if b, err = batcher.NewFromConfig(conf.Broker.Batching, b, mgr); err != nil {
			return nil, err
		}
		return b, nil
	}

	outputs := make([]output.Streamed, lOutputs)

	_, isRetryWrapped := map[string]struct{}{
		"fan_out":            {},
		"fan_out_sequential": {},
	}[conf.Broker.Pattern]

	var err error
	for j := 0; j < conf.Broker.Copies; j++ {
		for i, oConf := range outputConfs {
			oMgr := mgr.IntoPath("broker", "outputs", strconv.Itoa(i))
			tmpOut, err := oMgr.NewOutput(oConf)
			if err != nil {
				return nil, err
			}
			if isRetryWrapped {
				if tmpOut, err = RetryOutputIndefinitely(mgr, tmpOut); err != nil {
					return nil, err
				}
			}
			outputs[j*len(outputConfs)+i] = tmpOut
		}
	}

	var b output.Streamed
	switch conf.Broker.Pattern {
	case "fan_out":
		b, err = newFanOutOutputBroker(outputs)
	case "fan_out_sequential":
		b, err = newFanOutSequentialOutputBroker(outputs)
	case "round_robin":
		b, err = newRoundRobinOutputBroker(outputs)
	case "greedy":
		b, err = newGreedyOutputBroker(outputs)
	default:
		return nil, fmt.Errorf("broker pattern was not recognised: %v", conf.Broker.Pattern)
	}

	if !conf.Broker.Batching.IsNoop() {
		policy, err := policy.New(conf.Broker.Batching, mgr.IntoPath("broker", "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		b = batcher.New(policy, b, mgr)
	}
	return b, err
}
