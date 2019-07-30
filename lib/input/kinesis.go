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

package input

import (
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKinesis] = TypeSpec{
		constructor: NewKinesis,
		description: `
Receive messages from a Kinesis stream.

It's possible to use DynamoDB for persisting shard iterators by setting the
table name. Offsets will then be tracked per ` + "`client_id`" + ` per
` + "`shard_id`" + `. When using this mode you should create a table with
` + "`namespace`" + ` as the primary key and ` + "`shard_id`" + ` as a sort key.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
	}
}

//------------------------------------------------------------------------------

// NewKinesis creates a new AWS Kinesis input type.
func NewKinesis(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := reader.NewKinesis(conf.Kinesis, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader(
		"kinesis",
		reader.NewPreserver(k),
		log, stats,
	)
}

//------------------------------------------------------------------------------
