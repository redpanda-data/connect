package processor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/aws/session"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

func init() {
	Constructors[TypeAWSLambda] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newLambdaProc(conf.AWSLambda, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("aws_lambda", p, mgr.Metrics()), nil
		},
		Version: "3.36.0",
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Invokes an AWS lambda for each message. The contents of the message is the
payload of the request, and the result of the invocation will become the new
contents of the message.`,
		Description: `
The ` + "`rate_limit`" + ` field can be used to specify a rate limit [resource](/docs/components/rate_limits/about) to cap the rate of requests across parallel components service wide.

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
			docs.FieldDeprecated("parallel", "Whether messages of a batch should be dispatched in parallel.").HasDefault(true),
			docs.FieldCommon("function", "The function to invoke."),
			docs.FieldAdvanced("rate_limit", "An optional [`rate_limit`](/docs/components/rate_limits/about) to throttle invocations by."),
		}.Merge(session.FieldSpecs()).Add(
			docs.FieldAdvanced("timeout", "The maximum period of time to wait before abandoning an invocation."),
			docs.FieldAdvanced("retries", "The maximum number of retry attempts for each message."),
		),
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
	Parallel       bool `json:"parallel" yaml:"parallel"`
	session.Config `json:",inline" yaml:",inline"`
	Function       string `json:"function" yaml:"function"`
	Timeout        string `json:"timeout" yaml:"timeout"`
	NumRetries     int    `json:"retries" yaml:"retries"`
	RateLimit      string `json:"rate_limit" yaml:"rate_limit"`
}

// NewLambdaConfig returns a LambdaConfig with default values.
func NewLambdaConfig() LambdaConfig {
	return LambdaConfig{
		Parallel:   true,
		Config:     session.NewConfig(),
		Function:   "",
		Timeout:    "5s",
		NumRetries: 3,
		RateLimit:  "",
	}
}

//------------------------------------------------------------------------------

type lambdaProc struct {
	client   *lambdaClient
	parallel bool

	functionName string
	log          log.Modular
}

func newLambdaProc(conf LambdaConfig, mgr interop.Manager) (processor.V2Batched, error) {
	l := &lambdaProc{
		functionName: conf.Function,
		log:          mgr.Logger(),
		parallel:     conf.Parallel,
	}
	var err error
	if l.client, err = newLambdaClient(conf, mgr); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

func (l *lambdaProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, batch *message.Batch) ([]*message.Batch, error) {
	var resultMsg *message.Batch
	if !l.parallel || batch.Len() == 1 {
		resultMsg = batch.Copy()
		_ = resultMsg.Iter(func(i int, p *message.Part) error {
			if err := l.client.InvokeV2(p); err != nil {
				l.log.Errorf("Lambda function '%v' failed: %v\n", l.functionName, err)
				processor.MarkErr(p, spans[i], err)
			}
			return nil
		})
	} else {
		parts := make([]*message.Part, batch.Len())
		_ = batch.Iter(func(i int, p *message.Part) error {
			parts[i] = p.Copy()
			return nil
		})

		wg := sync.WaitGroup{}
		wg.Add(batch.Len())

		for i := 0; i < batch.Len(); i++ {
			go func(index int) {
				result := batch.Get(index).Copy()
				err := l.client.InvokeV2(result)
				if err != nil {
					l.log.Errorf("Lambda parallel request to '%v' failed: %v\n", l.functionName, err)
					processor.MarkErr(parts[index], spans[index], err)
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
	return msgs[:], nil
}

func (l *lambdaProc) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type lambdaClient struct {
	lambda *lambda.Lambda

	conf LambdaConfig
	log  log.Modular
	mgr  interop.Manager

	timeout time.Duration
}

func newLambdaClient(conf LambdaConfig, mgr interop.Manager) (*lambdaClient, error) {
	l := lambdaClient{
		conf: conf,
		log:  mgr.Logger(),
		mgr:  mgr,
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if l.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if conf.Function == "" {
		return nil, errors.New("lambda function must not be empty")
	}

	sess, err := l.conf.GetSession()
	if err != nil {
		return nil, err
	}

	if conf.RateLimit != "" {
		if !l.mgr.ProbeRateLimit(conf.RateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", conf.RateLimit)
		}
	}

	l.lambda = lambda.New(sess)
	return &l, nil
}

//------------------------------------------------------------------------------

func (l *lambdaClient) waitForAccess(ctx context.Context) bool {
	if l.conf.RateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := l.mgr.AccessRateLimit(ctx, l.conf.RateLimit, func(rl ratelimit.V1) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			l.log.Errorf("Rate limit error: %v\n", err)
			period = time.Second
		}
		if period > 0 {
			<-time.After(period)
		} else {
			return true
		}
	}
}

func (l *lambdaClient) InvokeV2(p *message.Part) error {
	remainingRetries := l.conf.NumRetries
	for {
		l.waitForAccess(context.Background())

		ctx, done := context.WithTimeout(context.Background(), l.timeout)
		result, err := l.lambda.InvokeWithContext(ctx, &lambda.InvokeInput{
			FunctionName: aws.String(l.conf.Function),
			Payload:      p.Get(),
		})
		done()
		if err == nil {
			if result.FunctionError != nil {
				p.MetaSet("lambda_function_error", *result.FunctionError)
			}
			p.Set(result.Payload)
			return nil
		}

		remainingRetries--
		if remainingRetries < 0 {
			return err
		}
	}
}
