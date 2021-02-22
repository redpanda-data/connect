package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGCPCloudStorage] = TypeSpec{
		constructor: fromSimpleConstructor(NewGCPCloudStorage),
		Status:      docs.StatusExperimental,
		Version:     "3.41.1",
		Summary: `
Sends message parts as objects to a Google Cloud Storage bucket. Each object is
uploaded with the path specified with the ` + "`path`" + ` field.`,
		Description: `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/gcp.md).

### Batching

It's common to want to upload messages to Google Cloud Storage as batched
archives, the easiest way to do this is to batch your messages at the output
level and join the batch of messages with an
` + "[`archive`](/docs/components/processors/archive)" + ` and/or
` + "[`compress`](/docs/components/processors/compress)" + ` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents
we could achieve that with the following config:

` + "```yaml" + `
output:
  gcp_cloud_storage:
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
  gcp_cloud_storage:
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
			docs.FieldCommon("content_type", "The content type to set for each object.").IsInterpolated(),
			docs.FieldAdvanced("content_encoding", "An optional content encoding to set for each object.").IsInterpolated(),
			docs.FieldAdvanced("chunk_size", "An optional chunk size which controls the maximum number of bytes of the object that the Writer will attempt to send to the server in a single request. If ChunkSize is set to zero, chunking will be disabled."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
			CategoryGCP,
		},
	}
}

//------------------------------------------------------------------------------

// NewGCPCloudStorage creates a new GCP Cloud Storage output type.
func NewGCPCloudStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	g, err := writer.NewGCPCloudStorage(conf.GCPCloudStorage, log, stats)
	if err != nil {
		return nil, err
	}

	w, err := NewAsyncWriter(TypeGCPCloudStorage, conf.GCPCloudStorage.MaxInFlight, g, log, stats)
	if err != nil {
		return nil, err
	}
	return newBatcherFromConf(conf.GCPCloudStorage.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
