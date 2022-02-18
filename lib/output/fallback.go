package output

import (
	"fmt"
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// TryConfig contains configuration fields for the Try output type.
type TryConfig brokerOutputList

// NewTryConfig creates a new BrokerConfig with default values.
func NewTryConfig() TryConfig {
	return TryConfig{}
}

func init() {
	Constructors[TypeFallback] = TypeSpec{
		constructor: newFallback,
		Version:     "3.58.0",
		Summary: `
Attempts to send each message to a child output, starting from the first output on the list. If an output attempt fails then the next output in the list is attempted, and so on.`,
		Description: `
This pattern is useful for triggering events in the case where certain output targets have broken. For example, if you had an output type ` + "`http_client`" + ` but wished to reroute messages whenever the endpoint becomes unreachable you could use this pattern:

` + "```yaml" + `
output:
  fallback:
    - http_client:
        url: http://foo:4195/post/might/become/unreachable
        retries: 3
        retry_period: 1s
    - http_client:
        url: http://bar:4196/somewhere/else
        retries: 3
        retry_period: 1s
      processors:
        - bloblang: 'root = "failed to send this message to foo: " + content()'
    - file:
        path: /usr/local/benthos/everything_failed.jsonl
` + "```" + `

### Batching

When an output within a fallback sequence uses batching, like so:

` + "```yaml" + `
output:
  fallback:
    - aws_dynamodb:
        table: foo
        string_columns:
          id: ${!json("id")}
          content: ${!content()}
        batching:
          count: 10
          period: 1s
    - file:
        path: /usr/local/benthos/failed_stuff.jsonl
` + "```" + `

Benthos makes a best attempt at inferring which specific messages of the batch failed, and only propagates those individual messages to the next fallback tier.

However, depending on the output and the error returned it is sometimes not possible to determine the individual messages that failed, in which case the whole batch is passed to the next tier in order to preserve at-least-once delivery guarantees.`,
		Categories: []Category{
			CategoryUtility,
		},
		config: docs.FieldComponent().Array().HasType(docs.FieldTypeOutput),
	}
}

//------------------------------------------------------------------------------

func newFallback(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...iprocessor.PipelineConstructorFunc,
) (output.Streamed, error) {
	pipelines = AppendProcessorsFromConfig(conf, mgr, log, stats, pipelines...)

	outputConfs := conf.Fallback

	if len(outputConfs) == 0 {
		return nil, ErrBrokerNoOutputs
	}
	outputs := make([]output.Streamed, len(outputConfs))

	maxInFlight := 1

	var err error
	for i, oConf := range outputConfs {
		oMgr := mgr.IntoPath("fallback", strconv.Itoa(i))
		if outputs[i], err = New(oConf, oMgr, oMgr.Logger(), oMgr.Metrics()); err != nil {
			return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Type, err)
		}
		if mif, ok := output.GetMaxInFlight(outputs[i]); ok && mif > maxInFlight {
			maxInFlight = mif
		}
	}

	if maxInFlight <= 1 {
		maxInFlight = 50
	}

	var t *broker.Try
	if t, err = broker.NewTry(outputs, stats); err != nil {
		return nil, err
	}
	t.WithMaxInFlight(maxInFlight)
	t.WithOutputMetricsPrefix("fallback.outputs")
	return WrapWithPipelines(t, pipelines...)
}

//------------------------------------------------------------------------------
