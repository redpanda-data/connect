package processor

import (
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/lambda/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSLambda] = TypeSpec{
		constructor: NewAWSLambda,
		Version:     "3.36.0",
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Invokes an AWS lambda for each message. The contents of the message is the
payload of the request, and the result of the invocation will become the new
contents of the message.`,
		Description: `
It is possible to perform requests per message of a batch in parallel by setting
the ` + "`parallel`" + ` flag to ` + "`true`" + `. The ` + "`rate_limit`" + `
field can be used to specify a rate limit [resource](/docs/components/rate_limits/about)
to cap the rate of requests across parallel components service wide.

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`branch` processor](/docs/components/processors/branch)" + `.

### Error Handling

When Benthos is unable to connect to the AWS endpoint or is otherwise unable to invoke the target lambda function it will retry the request according to the configured number of retries. Once these attempts have been exhausted the failed message will continue through the pipeline with it's contents unchanged, but flagged as having failed, allowing you to use [standard processor error handling patterns](/docs/configuration/error_handling).

However, if the invocation of the function is successful but the function itself throws an error, then the message will have it's contents updated with a JSON payload describing the reason for the failure, and a metadata field ` + "`lambda_function_error`" + ` will be added to the message allowing you to detect and handle function errors with a ` + "[`branch`](/docs/components/processors/branch)" + `:

` + "```yaml" + `
pipeline:
  processors:
    - branch:
        processors:
          - aws_lambda:
              function: foo
        result_map: |
          root = if meta().exists("lambda_function_error") {
            throw("Invocation failed due to %v: %v".format(this.errorType, this.errorMessage))
          } else {
            this
          }
output:
  switch:
    retry_until_success: false
    cases:
      - check: errored()
        output:
          reject: ${! error() }
      - output:
          resource: somewhere_else
` + "```" + `

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("parallel", "Whether messages of a batch should be dispatched in parallel."),
		}.Merge(client.FieldSpecs()),
		Examples: []docs.AnnotatedExample{
			{
				Title: "Branched Invoke",
				Summary: `
This example uses a ` + "[`branch` processor](/docs/components/processors/branch/)" + ` to map a new payload for triggering a lambda function with an ID and username from the original message, and the result of the lambda is discarded, meaning the original message is unchanged.`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: '{"id":this.doc.id,"username":this.user.name}'
        processors:
          - aws_lambda:
              function: trigger_user_update
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// LambdaConfig contains configuration fields for the Lambda processor.
type LambdaConfig struct {
	client.Config `json:",inline" yaml:",inline"`
	Parallel      bool `json:"parallel" yaml:"parallel"`
}

// NewLambdaConfig returns a LambdaConfig with default values.
func NewLambdaConfig() LambdaConfig {
	return LambdaConfig{
		Config:   client.NewConfig(),
		Parallel: false,
	}
}

//------------------------------------------------------------------------------

// Lambda is a processor that invokes an AWS Lambda using the message as the
// request body, and returns the response.
type Lambda struct {
	client *client.Type

	parallel bool

	conf  LambdaConfig
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrLambda metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewAWSLambda returns a Lambda processor.
func NewAWSLambda(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return newLambda(conf.AWSLambda, mgr, log, stats)
}

func newLambda(
	conf LambdaConfig, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	l := &Lambda{
		conf:  conf,
		log:   log,
		stats: stats,

		parallel: conf.Parallel,

		mCount:     stats.GetCounter("count"),
		mErrLambda: stats.GetCounter("error.lambda"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if l.client, err = client.New(
		conf.Config,
		client.OptSetLogger(l.log),
		// TODO: V4 Remove this
		client.OptSetStats(metrics.Namespaced(l.stats, "client")),
		client.OptSetManager(mgr),
	); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (l *Lambda) ProcessMessage(msg *message.Batch) ([]*message.Batch, types.Response) {
	l.mCount.Incr(1)

	var resultMsg *message.Batch
	if !l.parallel || msg.Len() == 1 {
		resultMsg = msg.Copy()
		IteratePartsWithSpanV2("aws_lambda", nil, resultMsg, func(i int, _ *tracing.Span, p *message.Part) error {
			if err := l.client.InvokeV2(p); err != nil {
				l.mErr.Incr(1)
				l.mErrLambda.Incr(1)
				l.log.Errorf("Lambda function '%v' failed: %v\n", l.conf.Config.Function, err)
				return err
			}
			return nil
		})
	} else {
		parts := make([]*message.Part, msg.Len())
		_ = msg.Iter(func(i int, p *message.Part) error {
			parts[i] = p.Copy()
			return nil
		})

		wg := sync.WaitGroup{}
		wg.Add(msg.Len())

		for i := 0; i < msg.Len(); i++ {
			go func(index int) {
				result := msg.Get(index).Copy()
				err := l.client.InvokeV2(result)
				if err != nil {
					l.mErr.Incr(1)
					l.mErrLambda.Incr(1)
					l.log.Errorf("Lambda parallel request to '%v' failed: %v\n", l.conf.Config.Function, err)
					FlagErr(parts[index], err)
				} else {
					parts[index] = result
				}

				wg.Done()
			}(i)
		}

		wg.Wait()
		resultMsg = message.QuickBatch(nil)
		resultMsg.SetAll(parts)
	}

	msgs := [1]*message.Batch{resultMsg}

	l.mBatchSent.Incr(1)
	l.mSent.Incr(int64(resultMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (l *Lambda) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (l *Lambda) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
