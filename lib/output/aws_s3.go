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
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: fromSimpleConstructor(NewAWSS3),
		Version:     "3.36.0",
		Summary: `
Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the ` + "`path`" + ` field.`,
		Description: `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Tags

Tag fields will be stored as object tags against the S3 object. For example:

` + "```yaml" + `
output:
    aws_s3:
        bucket: TODO
        path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
        tags:
            Key1: Value1
            Timestamp: ${!meta("Timestamp")}
` + "```" + `

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
  aws_s3:
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
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.AWSS3, conf.AWSS3.Batching)
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
			docs.FieldCommon(
				"tags", "Tags to provide to the object in S3.",
				map[string]string{
					"Key1":      "Value1",
					"Timestamp": `${!meta("Timestamp")}`,
				},
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

	Constructors[TypeS3] = TypeSpec{
		constructor: fromSimpleConstructor(NewAmazonS3),
		Status:      docs.StatusDeprecated,
		Summary: `
Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the ` + "`path`" + ` field.`,
		Description: `
## Alternatives

This output has been renamed to ` + "[`aws_s3`](/docs/components/outputs/aws_s3)" + `.

In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Tags

Tag fields will be stored as object tags against the S3 object. For example:

` + "```yaml" + `
output:
    aws_s3:
        bucket: TODO
        path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
        tags:
            Key1: Value1
            Timestamp: ${!meta("Timestamp")}
` + "```" + `

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
			docs.FieldCommon(
				"tags", "Tags to provide to the object in S3.",
				map[string]string{
					"Key1":      "Value1",
					"Timestamp": `${!meta("Timestamp")}`,
				},
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

// NewAWSS3 creates a new AmazonS3 output type.
func NewAWSS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonS3(TypeAWSS3, conf.AWSS3, mgr, log, stats)
}

// NewAmazonS3 creates a new AmazonS3 output type.
func NewAmazonS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newAmazonS3(TypeS3, conf.S3, mgr, log, stats)
}

func newAmazonS3(name string, conf writer.AmazonS3Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAmazonS3(conf, log, stats)
	if err != nil {
		return nil, err
	}

	w, err := NewAsyncWriter(name, conf.MaxInFlight, sthree, log, stats)
	if err != nil {
		return nil, err
	}
	return newBatcherFromConf(conf.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
