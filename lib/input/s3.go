package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeS3] = TypeSpec{
		constructor: fromSimpleConstructor(NewAmazonS3),
		Status:      docs.StatusDeprecated,
		Summary: `
Downloads objects within an Amazon S3 bucket, optionally filtered by a prefix.
If an SQS queue has been configured then only object keys read from the queue
will be downloaded.`,
		Description: `
## Alternatives

This input is being replaced with the shiny new ` + "[`aws_s3` input](/docs/components/inputs/aws_s3)" + `, which has improved features, consider trying it out instead.

If an SQS queue is not specified the entire list of objects found when this
input starts will be consumed. Note that the prefix configuration is only used
when downloading objects without SQS configured.

If your bucket is configured to send events directly to an SQS queue then you
need to set the ` + "`sqs_body_path`" + ` field to a
[dot path](/docs/configuration/field_paths) where the object key is found in the payload.
However, it is also common practice to send bucket events to an SNS topic which
sends enveloped events to SQS, in which case you must also set the
` + "`sqs_envelope_path`" + ` field to where the payload can be found.

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

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).

### Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- s3_key
- s3_bucket
- s3_last_modified_unix*
- s3_last_modified (RFC3339)*
- s3_content_type*
- s3_content_encoding*
- All user defined metadata*

* Only added when NOT using download manager
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("bucket", "The bucket to consume from. If `sqs_bucket_path` is set this field is still required as a fallback."),
				docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed. This field is ignored when SQS is used."),
				docs.FieldCommon("sqs_url", "An optional SQS URL to connect to. When specified this queue will control which objects are downloaded from the target bucket."),
				docs.FieldCommon("sqs_body_path", "A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages, this field is only required when an `sqs_url` is specified."),
				docs.FieldCommon("sqs_bucket_path", "An optional [dot path](/docs/configuration/field_paths) whereby the bucket of an object can be found in consumed SQS messages."),
				docs.FieldCommon("sqs_envelope_path", "An optional [dot path](/docs/configuration/field_paths) of enveloped payloads to extract from SQS messages. This is required when pushing events from S3 to SNS to SQS."),
				docs.FieldAdvanced("sqs_max_messages", "The maximum number of SQS messages to consume from each request."),
				docs.FieldAdvanced("sqs_endpoint", "A custom endpoint to use when connecting to SQS."),
			}, session.FieldSpecs()...),
			docs.FieldAdvanced("retries", "The maximum number of times to attempt an object download."),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs, which helps when connecting to custom endpoints."),
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket."),
			docs.FieldAdvanced("download_manager", "Controls if and how to use the download manager API. This can help speed up file downloads, but results in file metadata not being copied.").WithChildren(
				docs.FieldCommon("enabled", "Whether to use to download manager API."),
			),
			docs.FieldAdvanced("timeout", "The period of time to wait before abandoning a request and trying again."),
			docs.FieldDeprecated("max_batch_count"),
		),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// NewAmazonS3 creates a new AWS S3 input type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.S3.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use the batching methods outlined in https://benthos.dev/docs/configuration/batching instead.\n", conf.Type)
	}
	r, err := reader.NewAmazonS3(conf.S3, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeS3,
		true,
		reader.NewAsyncBundleUnacks(
			reader.NewAsyncPreserver(r),
		),
		log, stats, nil,
	)
}

//------------------------------------------------------------------------------
