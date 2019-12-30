// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPClient] = TypeSpec{
		constructor: NewHTTPClient,
		Description: `
Sends messages to an HTTP server. The request will be retried for each message
whenever the response code is outside the range of 200 -> 299 inclusive. It is
possible to list codes outside of this range in the ` + "`drop_on`" + ` field in
order to prevent retry attempts.

The period of time between retries is linear by default. Response codes that are
within the ` + "`backoff_on`" + ` list will instead apply exponential backoff
between retry attempts.

When the number of retries expires the output will reject the message, the
behaviour after this will depend on the pipeline but usually this simply means
the send is attempted again until successful whilst applying back pressure.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

The body of the HTTP request is the raw contents of the message payload. If the
message has multiple parts the request will be sent according to
[RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html)

### Propagating Responses

It's possible to propagate the response from each HTTP request back to the input
source by setting ` + "`propagate_response` to `true`" + `. Only inputs that
support [synchronous responses](../sync_responses.md) are able to make use of
these propagated responses.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.HTTPClient, conf.HTTPClient.Batching)
		},
		Async:   true,
		Batches: true,
	}
}

// NewHTTPClient creates a new HTTPClient output type.
func NewHTTPClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	h, err := writer.NewHTTPClient(conf.HTTPClient, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.HTTPClient.MaxInFlight == 1 {
		w, err = NewWriter(TypeHTTPClient, h, log, stats)
	} else {
		w, err = NewAsyncWriter(TypeHTTPClient, conf.HTTPClient.MaxInFlight, h, log, stats)
	}
	if bconf := conf.HTTPClient.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
