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
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["http"] = TypeSpec{
		constructor: NewHTTP,
		description: `
Performs an HTTP request using a message batch as the request body, and replaces
the original message parts with the body of the response.

If the batch contains only a single message part then it will be sent as the
body of the request. If the batch contains multiple messages then they will be
sent as a multipart HTTP request using the ` + "`Content-Type: multipart`" + `
header.

If you wish to avoid this behaviour then you can either use the
 ` + "[`archive`](#archive)" + ` processor to create a single message from a
batch, or use the ` + "[`split`](#split)" + ` processor to break down the batch
into individual message parts.

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`process_map`](#process_map)" + ` or
 ` + "[`process_field`](#process_field)" + ` processors.`,
	}
}

//------------------------------------------------------------------------------

// HTTPConfig contains any configuration for the HTTP processor.
type HTTPConfig struct {
	Client client.Config `json:"request" yaml:"request"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Client: client.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// HTTP is a processor that executes HTTP queries on a message part and
// replaces the contents with the result.
type HTTP struct {
	client *client.Type

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

		mCount:     stats.GetCounter("processor.http.count"),
		mSucc:      stats.GetCounter("processor.http.success"),
		mErr:       stats.GetCounter("processor.http.error"),
		mErrHTTP:   stats.GetCounter("processor.http.error.http"),
		mSent:      stats.GetCounter("processor.http.sent"),
		mSentParts: stats.GetCounter("processor.http.parts.sent"),
	}
	g.client = client.New(
		conf.HTTP.Client,
		client.OptSetLogger(g.log),
		client.OptSetStats(metrics.Namespaced(g.stats, "processor.http")),
	)
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage parses message parts as grok patterns.
func (h *HTTP) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	h.mCount.Incr(1)

	responseMsg, err := h.client.Send(msg)
	if err != nil {
		h.mErr.Incr(1)
		h.mErrHTTP.Incr(1)
		return nil, types.NewSimpleResponse(fmt.Errorf(
			"HTTP request '%v' failed: %v", h.conf.HTTP.Client.URL, err,
		))
	}

	if responseMsg.Len() < 1 {
		h.mErr.Incr(1)
		h.mErrHTTP.Incr(1)
		return nil, types.NewSimpleResponse(fmt.Errorf(
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
