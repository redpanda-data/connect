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
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTP] = TypeSpec{
		constructor: NewHTTP,
		description: `
Performs an HTTP request using a message batch as the request body, and replaces
the original message parts with the body of the response.

If the batch contains only a single message part then it will be sent as the
body of the request. If the batch contains multiple messages then they will be
sent as a multipart HTTP request using a ` + "`Content-Type: multipart`" + `
header.

If you are sending batches and wish to avoid this behaviour then you can set the
` + "`parallel`" + ` flag to ` + "`true`" + ` and the messages of a batch will
be sent as individual requests in parallel. You can also cap the max number of
parallel requests with ` + "`max_parallel`" + `. Alternatively, you can use the
` + "[`archive`](#archive)" + ` processor to create a single message
from the batch.

The ` + "`rate_limit`" + ` field can be used to specify a rate limit
[resource](../rate_limits/README.md) to cap the rate of requests across all
parallel components service wide.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`process_map`](#process_map)" + ` or
 ` + "[`process_field`](#process_field)" + ` processors.

### Metadata

If the request returns a response code this processor sets a metadata field
` + "`http_status_code`" + ` on all resulting messages.

If the field ` + "`copy_response_headers` is set to `true`" + ` then any headers
in the response will also be set in the resulting message as metadata.
 
### Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](../error_handling.md).`,
	}
}

//------------------------------------------------------------------------------

// HTTPConfig contains configuration fields for the HTTP processor.
type HTTPConfig struct {
	Client      client.Config `json:"request" yaml:"request"`
	Parallel    bool          `json:"parallel" yaml:"parallel"`
	MaxParallel int           `json:"max_parallel" yaml:"max_parallel"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Client:      client.NewConfig(),
		Parallel:    false,
		MaxParallel: 0,
	}
}

//------------------------------------------------------------------------------

// HTTP is a processor that performs an HTTP request using the message as the
// request body, and returns the response.
type HTTP struct {
	client *client.Type

	parallel bool
	max      int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrHTTP   metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewHTTP returns a HTTP processor.
func NewHTTP(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	g := &HTTP{
		conf:  conf,
		log:   log,
		stats: stats,

		parallel: conf.HTTP.Parallel,
		max:      conf.HTTP.MaxParallel,

		mCount:     stats.GetCounter("count"),
		mErrHTTP:   stats.GetCounter("error.http"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if g.client, err = client.New(
		conf.HTTP.Client,
		client.OptSetLogger(g.log),
		client.OptSetStats(metrics.Namespaced(g.stats, "client")),
		client.OptSetManager(mgr),
	); err != nil {
		return nil, err
	}
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (h *HTTP) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	h.mCount.Incr(1)
	var responseMsg types.Message

	if !h.parallel || msg.Len() == 1 {
		// Easy, just do a single request.
		resultMsg, err := h.client.Send(msg)
		if err != nil {
			var codeStr string
			if hErr, ok := err.(types.ErrUnexpectedHTTPRes); ok {
				codeStr = strconv.Itoa(hErr.Code)
			}
			h.mErr.Incr(1)
			h.mErrHTTP.Incr(1)
			h.log.Errorf("HTTP parallel request to '%v' failed: %v\n", h.conf.HTTP.Client.URL, err)
			responseMsg = msg.Copy()
			responseMsg.Iter(func(i int, p types.Part) error {
				if len(codeStr) > 0 {
					p.Metadata().Set("http_status_code", codeStr)
				}
				FlagErr(p, err)
				return nil
			})
		} else {
			parts := make([]types.Part, resultMsg.Len())
			resultMsg.Iter(func(i int, p types.Part) error {
				if i < msg.Len() {
					parts[i] = msg.Get(i).Copy()
				} else {
					parts[i] = msg.Get(0).Copy()
				}
				parts[i].Set(p.Get())
				p.Metadata().Iter(func(k, v string) error {
					parts[i].Metadata().Set(k, v)
					return nil
				})
				return nil
			})
			responseMsg = message.New(nil)
			responseMsg.Append(parts...)
		}
	} else {
		// Hard, need to do parallel requests limited by max parallelism.
		results := make([]types.Part, msg.Len())
		msg.Iter(func(i int, p types.Part) error {
			results[i] = p.Copy()
			return nil
		})
		reqChan, resChan := make(chan int), make(chan error)

		max := h.max
		if max == 0 || msg.Len() < max {
			max = msg.Len()
		}

		for i := 0; i < max; i++ {
			go func() {
				for index := range reqChan {
					result, err := h.client.Send(message.Lock(msg, index))
					if err == nil && result.Len() != 1 {
						err = fmt.Errorf("unexpected response size: %v", result.Len())
					}
					if err == nil {
						results[index].Set(result.Get(0).Get())
						result.Get(0).Metadata().Iter(func(k, v string) error {
							results[index].Metadata().Set(k, v)
							return nil
						})
					} else {
						if hErr, ok := err.(types.ErrUnexpectedHTTPRes); ok {
							results[index].Metadata().Set("http_status_code", strconv.Itoa(hErr.Code))
						}
						FlagErr(results[index], err)
					}
					resChan <- err
				}
			}()
		}
		go func() {
			for i := 0; i < msg.Len(); i++ {
				reqChan <- i
			}
		}()
		for i := 0; i < msg.Len(); i++ {
			if err := <-resChan; err != nil {
				h.mErr.Incr(1)
				h.mErrHTTP.Incr(1)
				h.log.Errorf("HTTP parallel request to '%v' failed: %v\n", h.conf.HTTP.Client.URL, err)
			}
		}

		close(reqChan)
		responseMsg = message.New(nil)
		responseMsg.Append(results...)
	}

	if responseMsg.Len() < 1 {
		return nil, response.NewError(fmt.Errorf(
			"HTTP response from '%v' was empty", h.conf.HTTP.Client.URL,
		))
	}

	msgs := [1]types.Message{responseMsg}

	h.mBatchSent.Incr(1)
	h.mSent.Incr(int64(responseMsg.Len()))
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (h *HTTP) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (h *HTTP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
