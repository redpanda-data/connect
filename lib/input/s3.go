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
	Constructors[TypeS3] = TypeSpec{
		constructor: NewAmazonS3,
		description: `
Downloads objects in an Amazon S3 bucket, optionally filtered by a prefix. If an
SQS queue has been configured then only object keys read from the queue will be
downloaded. Otherwise, the entire list of objects found when this input is
created will be downloaded. Note that the prefix configuration is only used when
downloading objects without SQS configured.

If the download manager is enabled this can help speed up file downloads but
results in file metadata not being copied.

If your bucket is configured to send events directly to an SQS queue then you
need to set the ` + "`sqs_body_path`" + ` field to where the object key is found
in the payload. However, it is also common practice to send bucket events to an
SNS topic which sends enveloped events to SQS, in which case you must also set
the ` + "`sqs_envelope_path`" + ` field to where the payload can be found.

When using SQS events it's also possible to extract target bucket names from the
events by specifying a path in the field ` + "`sqs_bucket_path`" + `. For each
SQS event, if that path exists and contains a string it will used as the bucket
of the download instead of the ` + "`bucket`" + ` field.

Here is a guide for setting up an SQS queue that receives events for new S3
bucket objects:

https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html

WARNING: When using SQS please make sure you have sensible values for
` + "`sqs_max_messages`" + ` and also the visibility timeout of the queue
itself.

When Benthos consumes an S3 item as a result of receiving an SQS message the
message is not deleted until the S3 item has been sent onwards. This ensures
at-least-once crash resiliency, but also means that if the S3 item takes longer
to process than the visibility timeout of your queue then the same items might
be processed multiple times.

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- s3_key
- s3_bucket
- s3_last_modified_unix*
- s3_last_modified (RFC3339)*
- s3_content_type*
- All user defined metadata*

* Only added when NOT using download manager
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// NewAmazonS3 creates a new AWS S3 input type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	r, err := reader.NewAmazonS3(conf.S3, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader(
		"s3",
		reader.NewPreserver(r),
		log, stats,
	)
}

//------------------------------------------------------------------------------
