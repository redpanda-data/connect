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
	"errors"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["amazon_s3"] = TypeSpec{
		constructor: NewAmazonS3,
		description: `
Downloads objects in an Amazon S3 bucket, optionally filtered by a prefix. If an
SQS queue has been configured then only object keys read from the queue will be
downloaded. Otherwise, the entire list of objects found when this input is
created will be downloaded. Note that the prefix configuration is only used when
downloading objects without SQS configured.

If your bucket is configured to send events directly to an SQS queue then you
need to set the 'sqs_body_path' field to where the object key is found in the
payload. However, it is also common practice to send bucket events to an SNS
topic which sends enveloped events to SQS, in which case you must also set the
'sqs_envelope_path' field to where the payload can be found.

Here is a guide for setting up an SQS queue that receives events for new S3
bucket objects:

https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html`,
	}
}

//------------------------------------------------------------------------------

// NewAmazonS3 creates a new amazon S3 input type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.AmazonS3.Bucket) == 0 {
		return nil, errors.New("invalid bucket (cannot be empty)")
	}
	return NewReader(
		"amazon_s3",
		reader.NewPreserver(
			reader.NewAmazonS3(conf.AmazonS3, log, stats),
		),
		log, stats,
	)
}

//------------------------------------------------------------------------------
