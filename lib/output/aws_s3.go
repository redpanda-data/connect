package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
)

func init() {
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			return newAmazonS3(TypeAWSS3, conf.AWSS3, mgr, log, stats)
		}),
		Version: "3.36.0",
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

The tags field allows you to specify key/value pairs to attach to objects as tags, where the values support
[interpolation functions](/docs/configuration/interpolation#bloblang-queries):

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
[in this document](/docs/guides/cloud/aws).

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
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("bucket", "The bucket to upload messages to."),
			docs.FieldCommon(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).IsInterpolated(),
			docs.FieldString(
				"tags", "Key/value pairs to store with the object as tags.",
				map[string]string{
					"Key1":      "Value1",
					"Timestamp": `${!meta("Timestamp")}`,
				},
			).IsInterpolated().Map(),
			docs.FieldCommon("content_type", "The content type to set for each object.").IsInterpolated(),
			docs.FieldAdvanced("content_encoding", "An optional content encoding to set for each object.").IsInterpolated(),
			docs.FieldString("cache_control", "The cache control to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("content_disposition", "The content disposition to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("content_language", "The content language to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("website_redirect_location", "The website redirect location to set for each object.").Advanced().IsInterpolated(),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are attached to objects as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			docs.FieldAdvanced("storage_class", "The storage class to set for each object.").HasOptions(
				"STANDARD", "REDUCED_REDUNDANCY", "GLACIER", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "DEEP_ARCHIVE",
			).IsInterpolated(),
			docs.FieldAdvanced("kms_key_id", "An optional server side encryption key."),
			docs.FieldAdvanced("server_side_encryption", "An optional server side encryption algorithm.").AtVersion("3.63.0"),
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

func newAmazonS3(name string, conf writer.AmazonS3Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	sthree, err := writer.NewAmazonS3V2(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}

	w, err := NewAsyncWriter(name, conf.MaxInFlight, sthree, log, stats)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.Batching, w, mgr, log, stats)
}
