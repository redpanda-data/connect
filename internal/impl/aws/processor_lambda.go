// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

func init() {
	conf := service.NewConfigSpec().
		Stable().
		Summary("Invokes an AWS lambda for each message. The contents of the message is the payload of the request, and the result of the invocation will become the new contents of the message.").
		Description(`The `+"`rate_limit`"+` field can be used to specify a rate limit xref:components:rate_limits/about.adoc[resource] to cap the rate of requests across parallel components service wide.

In order to map or encode the payload to a specific request body, and map the response back into the original payload instead of replacing it entirely, you can use the `+"xref:components:processors/branch.adoc[`branch` processor]"+`.

== Error handling

When Redpanda Connect is unable to connect to the AWS endpoint or is otherwise unable to invoke the target lambda function it will retry the request according to the configured number of retries. Once these attempts have been exhausted the failed message will continue through the pipeline with it's contents unchanged, but flagged as having failed, allowing you to use xref:configuration:error_handling.adoc[standard processor error handling patterns].

However, if the invocation of the function is successful but the function itself throws an error, then the message will have it's contents updated with a JSON payload describing the reason for the failure, and a metadata field `+"`lambda_function_error`"+` will be added to the message allowing you to detect and handle function errors with a `+"xref:components:processors/branch.adoc[`branch`]"+`:

`+"```yaml"+`
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
`+"```"+`

== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[].`).
		Categories("Integration").
		Version("3.36.0").
		Example(
			"Branched Invoke",
			`
This example uses a `+"xref:components:processors/branch.adoc[`branch` processor]"+` to map a new payload for triggering a lambda function with an ID and username from the original message, and the result of the lambda is discarded, meaning the original message is unchanged.`,
			`
pipeline:
  processors:
    - branch:
        request_map: '{"id":this.doc.id,"username":this.user.name}'
        processors:
          - aws_lambda:
              function: trigger_user_update
`,
		).
		Field(service.NewBoolField("parallel").
			Description("Whether messages of a batch should be dispatched in parallel.").
			Default(false)).
		Field(service.NewStringField("function").
			Description("The function to invoke.")).
		Field(service.NewStringField("rate_limit").
			Description("An optional xref:components:rate_limits/about.adoc[`rate_limit`] to throttle invocations by.").
			Default("").
			Advanced())

	for _, f := range config.SessionFields() {
		conf = conf.Field(f)
	}

	conf = conf.Field(service.NewDurationField("timeout").
		Description("The maximum period of time to wait before abandoning an invocation.").
		Default("5s").
		Advanced())
	conf = conf.Field(service.NewIntField("retries").
		Description("The maximum number of retry attempts for each message.").
		Default(3).
		Advanced())

	service.MustRegisterBatchProcessor(
		"aws_lambda", conf,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			aconf, err := GetSession(context.TODO(), conf)
			if err != nil {
				return nil, err
			}

			parallel, err := conf.FieldBool("parallel")
			if err != nil {
				return nil, err
			}

			function, err := conf.FieldString("function")
			if err != nil {
				return nil, err
			}

			numRetries, err := conf.FieldInt("retries")
			if err != nil {
				return nil, err
			}

			rateLimit, err := conf.FieldString("rate_limit")
			if err != nil {
				return nil, err
			}

			timeout, err := conf.FieldDuration("timeout")
			if err != nil {
				return nil, err
			}

			return newLambdaProc(lambda.NewFromConfig(aconf), parallel, function, numRetries, rateLimit, timeout, mgr)
		})
}

//------------------------------------------------------------------------------

type lambdaAPI interface {
	Invoke(context.Context, *lambda.InvokeInput, ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

type lambdaProc struct {
	client   *lambdaClient
	parallel bool

	functionName string
	log          *service.Logger
}

func newLambdaProc(
	lambda lambdaAPI,
	parallel bool,
	function string,
	numRetries int,
	rateLimit string,
	timeout time.Duration,
	mgr *service.Resources,
) (*lambdaProc, error) {
	l := &lambdaProc{
		functionName: function,
		log:          mgr.Logger(),
		parallel:     parallel,
	}
	var err error
	if l.client, err = newLambdaClient(lambda, function, numRetries, rateLimit, timeout, mgr); err != nil {
		return nil, err
	}
	return l, nil
}

//------------------------------------------------------------------------------

func (l *lambdaProc) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if !l.parallel || len(batch) == 1 {
		for _, p := range batch {
			if err := l.client.InvokeV2(p); err != nil {
				l.log.Errorf("Lambda function '%v' failed: %v\n", l.functionName, err)
				p.SetError(err)
			}
		}
	} else {
		wg := sync.WaitGroup{}
		wg.Add(len(batch))

		for i := 0; i < len(batch); i++ {
			go func(index int) {
				err := l.client.InvokeV2(batch[index])
				if err != nil {
					l.log.Errorf("Lambda parallel request to '%v' failed: %v\n", l.functionName, err)
					batch[index].SetError(err)
				}
				wg.Done()
			}(i)
		}

		wg.Wait()
	}

	return []service.MessageBatch{batch}, nil
}

func (*lambdaProc) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type lambdaClient struct {
	lambda lambdaAPI

	log *service.Logger
	mgr *service.Resources

	function  string
	retries   int
	rateLimit string
	timeout   time.Duration
}

func newLambdaClient(
	lambda lambdaAPI,
	function string,
	numRetries int,
	rateLimit string,
	timeout time.Duration,
	mgr *service.Resources,
) (*lambdaClient, error) {
	l := lambdaClient{
		lambda:    lambda,
		log:       mgr.Logger(),
		mgr:       mgr,
		function:  function,
		retries:   numRetries,
		rateLimit: rateLimit,
		timeout:   timeout,
	}
	if function == "" {
		return nil, errors.New("lambda function must not be empty")
	}

	if rateLimit != "" {
		if !l.mgr.HasRateLimit(rateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", rateLimit)
		}
	}

	return &l, nil
}

//------------------------------------------------------------------------------

func (l *lambdaClient) waitForAccess(ctx context.Context) bool {
	if l.rateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := l.mgr.AccessRateLimit(ctx, l.rateLimit, func(rl service.RateLimit) {
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

func (l *lambdaClient) InvokeV2(p *service.Message) error {
	remainingRetries := l.retries
	for {
		l.waitForAccess(context.Background())

		mBytes, err := p.AsBytes()
		if err != nil {
			return err
		}

		ctx, done := context.WithTimeout(context.Background(), l.timeout)
		result, err := l.lambda.Invoke(ctx, &lambda.InvokeInput{
			FunctionName: aws.String(l.function),
			Payload:      mBytes,
		})
		done()
		if err == nil {
			if result.FunctionError != nil {
				p.MetaSet("lambda_function_error", *result.FunctionError)
			}
			p.SetBytes(result.Payload)
			return nil
		}

		remainingRetries--
		if remainingRetries < 0 {
			return err
		}
	}
}
