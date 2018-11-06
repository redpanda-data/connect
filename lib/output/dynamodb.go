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
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDynamoDB] = TypeSpec{
		constructor: NewDynamoDB,
		description: `
Inserts messages into a DynamoDB table. Columns are populated by writing a map
of key/value pairs, where the values are
[function interpolated](../config_interpolation.md#functions) strings calculated
per message of a batch. This allows you to populate columns by extracting
fields within the document payload or metadata like follows:

` + "``` yaml" + `
type: dynamodb
dynamodb:
  table: foo
  string_columns:
    id: ${!json_field:id}
    title: ${!json_field:body.title}
    topic: ${!metadata:kafka_topic}
    full_content: ${!content}
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// NewDynamoDB creates a new DynamoDB output type.
func NewDynamoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	dyn, err := writer.NewDynamoDB(conf.DynamoDB, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		"dynamodb", dyn, log, stats,
	)
}

//------------------------------------------------------------------------------
