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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/client"
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
 ` + "[`process_field`](#process_field)" + ` processors.`,
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
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewHTTP returns a HTTP processor.
func NewHTTP(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	g := &HTTP{
		conf:  conf,
		log:   log.NewModule(".processor.http"),
		stats: stats,

		parallel: conf.HTTP.Parallel,
		max:      conf.HTTP.MaxParallel,

		mCount:     stats.GetCounter("processor.http.count"),
		mSucc:      stats.GetCounter("processor.http.success"),
		mErr:       stats.GetCounter("processor.http.error"),
		mErrHTTP:   stats.GetCounter("processor.http.error.http"),
		mSent:      stats.GetCounter("processor.http.sent"),
		mSentParts: stats.GetCounter("processor.http.parts.sent"),
	}
	var err error
	if g.client, err = client.New(
		conf.HTTP.Client,
		client.OptSetLogger(g.log),
		client.OptSetStats(metrics.Namespaced(g.stats, "processor.http")),
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
		var err error
		if responseMsg, err = h.client.Send(msg); err != nil {
			if err != nil {
				h.mErr.Incr(1)
				h.mErrHTTP.Incr(1)
				return nil, response.NewError(fmt.Errorf(
					"HTTP request '%v' failed: %v", h.conf.HTTP.Client.URL, err,
				))
			}
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
						results[index] = result.Get(0)
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
				h.mErrHTTP.Incr(1)
				h.log.Errorf("HTTP parallel request to '%v' failed: %v\n", h.conf.HTTP.Client.URL, err)
			}
		}

		close(reqChan)
		responseMsg = message.New(nil)
		responseMsg.Append(results...)
	}

	if responseMsg.Len() < 1 {
		h.mErr.Incr(1)
		h.mErrHTTP.Incr(1)
		return nil, response.NewError(fmt.Errorf(
			"HTTP response from '%v' was empty", h.conf.HTTP.Client.URL,
		))
	}

	h.mSucc.Incr(1)
	msgs := [1]types.Message{responseMsg}

	h.mSent.Incr(1)
	h.mSentParts.Incr(int64(responseMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
