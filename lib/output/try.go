package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeTry] = TypeSpec{
		brokerConstructor: NewTry,
		Description: `
Attempts to send each message to only one output, starting from the first output
on the list. If an output attempt fails then the next output in the list is
attempted, and so on.

This pattern is useful for triggering events in the case where certain output
targets have broken. For example, if you had an output type ` + "`http_client`" + `
but wished to reroute messages whenever the endpoint becomes unreachable you
could use this pattern:

` + "``` yaml" + `
output:
  try:
  - http_client:
      url: http://foo:4195/post/might/become/unreachable
      retries: 3
      retry_period: 1s
  - http_client:
      url: http://bar:4196/somewhere/else
      retries: 3
      retry_period: 1s
    processors:
    - text:
        operator: prepend
        value: 'failed to send this message to foo: '
  - file:
      path: /usr/local/benthos/everything_failed.jsonl
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			outSlice := []interface{}{}
			for _, output := range conf.Try {
				sanOutput, err := SanitiseConfig(output)
				if err != nil {
					return nil, err
				}
				outSlice = append(outSlice, sanOutput)
			}
			return outSlice, nil
		},
	}
}

//------------------------------------------------------------------------------

// TryConfig contains configuration fields for the Try output type.
type TryConfig brokerOutputList

// NewTryConfig creates a new BrokerConfig with default values.
func NewTryConfig() TryConfig {
	return TryConfig{}
}

//------------------------------------------------------------------------------

// NewTry creates a new try broker output type.
func NewTry(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	outputConfs := conf.Try

	if len(outputConfs) == 0 {
		return nil, ErrBrokerNoOutputs
	}
	outputs := make([]types.Output, len(outputConfs))

	var err error
	for i, oConf := range outputConfs {
		ns := fmt.Sprintf("try.%v", i)
		var pipes []types.PipelineConstructorFunc
		outputs[i], err = New(
			oConf, mgr,
			log.NewModule("."+ns),
			metrics.Combine(stats, metrics.Namespaced(stats, ns)),
			pipes...)
		if err != nil {
			return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Type, err)
		}
	}

	var t *broker.Try
	if t, err = broker.NewTry(outputs, stats); err != nil {
		return nil, err
	}

	t.WithOutputMetricsPrefix("try.outputs")
	return WrapWithPipelines(t, pipelines...)
}

//------------------------------------------------------------------------------
