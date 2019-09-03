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
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSQS] = TypeSpec{
		constructor: NewAmazonSQS,
		description: `
Sends messages to an SQS queue. Metadata values are sent along with the payload
as attributes with the data type String. If the number of metadata values in a
message exceeds the message attribute limit (10) then the top ten keys ordered
alphabetically will be selected.

The fields ` + "`message_group_id` and `message_deduplication_id`" + ` can be
set dynamically using
[function interpolations](../config_interpolation.md#functions), which are
resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).`,
	}
}

//------------------------------------------------------------------------------

// NewAmazonSQS creates a new AmazonSQS output type.
func NewAmazonSQS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewAmazonSQS(conf.SQS, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"sqs", s, log, stats,
	)
}

//------------------------------------------------------------------------------
