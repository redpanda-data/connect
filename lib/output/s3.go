package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeS3] = TypeSpec{
		constructor: NewAmazonS3,
		Summary: `
Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the ` + "`path`" + ` field.`,
		Description: `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).

### Batching

It's common to want to upload messages to S3 as batched archives, the easiest
way to do this is to batch your messages at the output level and join the batch
of messages with an
` + "[`archive`](/docs/components/processors/archive)" + ` and/or
` + "[`compress`](/docs/components/processors/compress)" + ` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents
we could achieve that with the following config:

` + "```yaml" + `
output:
  s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip
` + "```" + `

Alternatively, if we wished to upload JSON documents as a single large document
containing an array of objects we can do that with:

` + "```yaml" + `
output:
  s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.S3, conf.S3.Batching)
		},
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("bucket", "The bucket to upload messages to."),
			docs.FieldCommon(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).SupportsInterpolation(false),
			docs.FieldCommon("content_type", "The content type to set for each object.").SupportsInterpolation(false),
			docs.FieldAdvanced("content_encoding", "An optional content encoding to set for each object.").SupportsInterpolation(false),
			docs.FieldAdvanced("storage_class", "The storage class to set for each object.").HasOptions(
				"STANDARD", "REDUCED_REDUNDANCY", "GLACIER", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "DEEP_ARCHIVE",
			).SupportsInterpolation(false),
			docs.FieldAdvanced("kms_key_id", "An optional server side encryption key."),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs, which helps when connecting to custom endpoints."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
			batch.FieldSpec(),
		}.Merge(session.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// NewAmazonS3 creates a new AmazonS3 output type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAmazonS3(conf.S3, log, stats)
	if err != nil {
		return nil, err
	}

	w, err := NewAsyncWriter(
		TypeS3, conf.S3.MaxInFlight, sthree, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return newBatcherFromConf(conf.S3.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
