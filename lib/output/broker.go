package output

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var (
	// ErrBrokerNoOutputs is returned when creating a Broker type with zero
	// outputs.
	ErrBrokerNoOutputs = errors.New("attempting to create broker output type with no outputs")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBroker] = TypeSpec{
		brokerConstructor: NewBroker,
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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced("copies", "The number of copies of each configured output to spawn."),
			docs.FieldCommon("pattern", "The brokering pattern to use.").HasOptions(
				"fan_out", "fan_out_sequential", "round_robin", "greedy", "try",
			),
			docs.FieldCommon(
				"max_in_flight",
				"The maximum number of messages to dispatch at any given time. Only relevant for `fan_out`, `fan_out_sequential` brokers.",
			),
			docs.FieldCommon("outputs", "A list of child outputs to broker."),
			batch.FieldSpec(),
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			nestedOutputs := conf.Broker.Outputs
			outSlice := []interface{}{}
			for _, output := range nestedOutputs {
				sanOutput, err := SanitiseConfig(output)
				if err != nil {
					return nil, err
				}
				outSlice = append(outSlice, sanOutput)
			}
			batchSanit, err := batch.SanitisePolicyConfig(conf.Broker.Batching)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"copies":        conf.Broker.Copies,
				"pattern":       conf.Broker.Pattern,
				"max_in_flight": conf.Broker.MaxInFlight,
				"outputs":       outSlice,
				"batching":      batchSanit,
			}, nil
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// BrokerConfig contains configuration fields for the Broker output type.
type BrokerConfig struct {
	Copies      int                `json:"copies" yaml:"copies"`
	Pattern     string             `json:"pattern" yaml:"pattern"`
	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	Outputs     brokerOutputList   `json:"outputs" yaml:"outputs"`
	Batching    batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Copies:      1,
		Pattern:     "fan_out",
		MaxInFlight: 1,
		Outputs:     brokerOutputList{},
		Batching:    batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// NewBroker creates a new Broker output type. Messages will be sent out to the
// list of outputs according to the chosen broker pattern.
func NewBroker(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	outputConfs := conf.Broker.Outputs

	lOutputs := len(outputConfs) * conf.Broker.Copies

	if lOutputs <= 0 {
		return nil, ErrBrokerNoOutputs
	}
	if lOutputs == 1 {
		b, err := New(outputConfs[0], mgr, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
		if b, err = newBatcherFromConf(conf.Broker.Batching, b, mgr, log, stats); err != nil {
			return nil, err
		}
		return b, nil
	}

	outputs := make([]types.Output, lOutputs)

	_, isThreaded := map[string]struct{}{
		"round_robin": {},
		"greedy":      {},
	}[conf.Broker.Pattern]

	var err error
	for j := 0; j < conf.Broker.Copies; j++ {
		for i, oConf := range outputConfs {
			ns := fmt.Sprintf("broker.outputs.%v", i)
			var pipes []types.PipelineConstructorFunc
			if isThreaded {
				pipes = pipelines
			}
			outputs[j*len(outputConfs)+i], err = New(
				oConf, mgr,
				log.NewModule("."+ns),
				metrics.Combine(stats, metrics.Namespaced(stats, ns)),
				pipes...)
			if err != nil {
				return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Type, err)
			}
		}
	}

	var b Type
	switch conf.Broker.Pattern {
	case "fan_out":
		var bTmp *broker.FanOut
		if bTmp, err = broker.NewFanOut(outputs, log, stats); err == nil {
			b = bTmp.WithMaxInFlight(conf.Broker.MaxInFlight)
		}
	case "fan_out_sequential":
		var bTmp *broker.FanOutSequential
		if bTmp, err = broker.NewFanOutSequential(outputs, log, stats); err == nil {
			b = bTmp.WithMaxInFlight(conf.Broker.MaxInFlight)
		}
	case "round_robin":
		b, err = broker.NewRoundRobin(outputs, stats)
	case "greedy":
		b, err = broker.NewGreedy(outputs)
	case "try":
		b, err = broker.NewTry(outputs, stats)
	default:
		return nil, fmt.Errorf("broker pattern was not recognised: %v", conf.Broker.Pattern)
	}
	if err == nil && !isThreaded {
		b, err = WrapWithPipelines(b, pipelines...)
	}

	if !conf.Broker.Batching.IsNoop() {
		policy, err := batch.NewPolicy(conf.Broker.Batching, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		b = NewBatcher(policy, b, log, stats)
	}
	return b, err
}

//------------------------------------------------------------------------------
