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

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedisStreams] = TypeSpec{
		constructor: NewRedisStreams,
		description: `
Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The
` + "`client_id`" + ` should be unique for each consumer of a group.

The field ` + "`limit`" + ` specifies the maximum number of records to be
received per request. When more than one record is returned they are batched and
can be split into individual messages with the ` + "`split`" + ` processor.

Redis stream entries are key/value pairs, as such it is necessary to specify the
key that contains the body of the message. All other keys/value pairs are saved
as metadata fields.`,
	}
}

//------------------------------------------------------------------------------

// NewRedisStreams creates a new Redis List input type.
func NewRedisStreams(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	r, err := reader.NewRedisStreams(conf.RedisStreams, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("redis_streams", reader.NewPreserver(r), log, stats)
}

//------------------------------------------------------------------------------
