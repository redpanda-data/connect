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

package output

import (
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSyncResponse] = TypeSpec{
		constructor: func(_ Config, _ types.Manager, logger log.Modular, stats metrics.Type) (Type, error) {
			return NewWriter(TypeSyncResponse, roundtrip.Writer{}, logger, stats)
		},
		description: `
Returns the final message payload back to the input origin of the message, where
it is dealt with according to that specific input type.

For most inputs this mechanism is ignored entirely, in which case the sync
response is dropped without penalty. It is therefore safe to use this output
even when combining input types that might not have support for sync responses.
An example of an input able to utilise this is the ` + "`http_server`" + `.

It is safe to combine this output with others using broker types. For example,
with the ` + "`http_server`" + ` input we could send the payload to a Kafka
topic and also send a modified payload back with:

` + "``` yaml" + `
input:
  http_server:
    path: /post
output:
  broker:
    pattern: fan_out
    outputs:
    - kafka:
        addresses: [ TODO:9092 ]
        topic: foo_topic
    - type: sync_response
      processors:
      - text:
          operator: to_upper
` + "```" + `

Using the above example and posting the message 'hello world' to the endpoint
` + "`/post`" + ` Benthos would send it unchanged to the topic
` + "`foo_topic`" + ` and also respond with 'HELLO WORLD'.

For more information please read [Synchronous Responses](../sync_responses.md).`,
	}
}

//------------------------------------------------------------------------------
