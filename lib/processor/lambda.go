// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/lambda/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeLambda] = TypeSpec{
		constructor: NewLambda,
		description: `
Invokes an AWS lambda for each message part of a batch. The contents of the
message part is the payload of the request, and the result of the invocation
will become the new contents of the message.

It is possible to perform requests per message of a batch in parallel by setting
the ` + "`parallel`" + ` flag to ` + "`true`" + `. The ` + "`rate_limit`" + `
field can be used to specify a rate limit [resource](../rate_limits/README.md)
to cap the rate of requests across parallel components service wide.

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`process_map`](#process_map)" + ` or
 ` + "[`process_field`](#process_field)" + ` processors.

### Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](../error_handling.md).

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
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

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrLambda metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewLambda returns a Lambda processor.
func NewLambda(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	l := &Lambda{
		conf:  conf,
		log:   log,
		stats: stats,

		parallel: conf.Lambda.Parallel,

		mCount:     stats.GetCounter("count"),
		mErrLambda: stats.GetCounter("error.lambda"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if l.client, err = client.New(
		conf.Lambda.Config,
		client.OptSetLogger(l.log),
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
func (l *Lambda) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	l.mCount.Incr(1)
	var responseMsg types.Message

	if !l.parallel || msg.Len() == 1 {
		// Easy, just do a single request.
		var err error
		if responseMsg, err = l.client.Invoke(msg); err != nil {
			l.mErr.Incr(1)
			l.mErrLambda.Incr(1)
			l.log.Errorf("Lambda function '%v' failed: %v\n", l.conf.Lambda.Config.Function, err)
			responseMsg = msg
			responseMsg.Iter(func(i int, p types.Part) error {
				FlagErr(p, err)
				return nil
			})
		}
	} else {
		parts := make([]types.Part, msg.Len())
		msg.Iter(func(i int, p types.Part) error {
			parts[i] = p.Copy()
			return nil
		})

		wg := sync.WaitGroup{}
		wg.Add(msg.Len())

		for i := 0; i < msg.Len(); i++ {
			go func(index int) {
				result, err := l.client.Invoke(message.Lock(msg, index))
				if err == nil && result.Len() != 1 {
					err = fmt.Errorf("unexpected response size: %v", result.Len())
				}
				if err != nil {
					l.mErr.Incr(1)
					l.mErrLambda.Incr(1)
					l.log.Errorf("Lambda parallel request to '%v' failed: %v\n", l.conf.Lambda.Config.Function, err)
					FlagErr(parts[index], err)
				} else {
					parts[index] = result.Get(0)
				}

				wg.Done()
			}(i)
		}

		wg.Wait()
		responseMsg = message.New(nil)
		responseMsg.SetAll(parts)
	}

	if responseMsg.Len() < 1 {
		l.mErr.Incr(1)
		l.log.Errorf("Lambda response from '%v' was empty", l.conf.Lambda.Config.Function)
		return nil, response.NewError(fmt.Errorf(
			"lambda response from '%v' was empty", l.conf.Lambda.Config.Function,
		))
	}

	msgs := [1]types.Message{responseMsg}

	l.mBatchSent.Incr(1)
	l.mSent.Incr(int64(responseMsg.Len()))
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
