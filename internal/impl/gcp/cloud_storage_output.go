package gcp

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	ioutput "github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
)

func init() {
	bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c output.Config, nm bundle.NewManagement) (output.Type, error) {
		g, err := newGCPCloudStorageOutput(c.GCPCloudStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter(output.TypeGCPCloudStorage, c.GCPCloudStorage.MaxInFlight, g, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		w = output.OnlySinglePayloads(w)
		return output.NewBatcherFromConfig(c.GCPCloudStorage.Batching, w, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    output.TypeGCPCloudStorage,
		Type:    docs.TypeOutput,
		Status:  docs.StatusBeta,
		Version: "3.43.0",
		Categories: []string{
			string(input.CategoryServices),
			string(input.CategoryGCP),
		},
		Summary: `
Sends message parts as objects to a Google Cloud Storage bucket. Each object is
uploaded with the path specified with the ` + "`path`" + ` field.`,
		Description: ioutput.Description(true, true, `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/gcp).

### Batching

It's common to want to upload messages to Google Cloud Storage as batched
archives, the easiest way to do this is to batch your messages at the output
level and join the batch of messages with an
`+"[`archive`](/docs/components/processors/archive)"+` and/or
`+"[`compress`](/docs/components/processors/compress)"+` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents
we could achieve that with the following config:

`+"```yaml"+`
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
`+"```"+`

Alternatively, if we wished to upload JSON documents as a single large document
containing an array of objects we can do that with:

`+"```yaml"+`
output:
  gcp_cloud_storage:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
`+"```"+``),
		Config: docs.FieldComponent().WithChildren(
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
			batch.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewGCPCloudStorageConfig()),
	})
}

// gcpCloudStorageOutput is a benthos writer.Type implementation that writes
// messages to a GCP Cloud Storage bucket.
type gcpCloudStorageOutput struct {
	conf output.GCPCloudStorageConfig

	path            *field.Expression
	contentType     *field.Expression
	contentEncoding *field.Expression

	client  *storage.Client
	connMut sync.RWMutex

	log   log.Modular
	stats metrics.Type
}

// newGCPCloudStorageOutput creates a new GCP Cloud Storage bucket writer.Type.
func newGCPCloudStorageOutput(
	conf output.GCPCloudStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*gcpCloudStorageOutput, error) {
	g := &gcpCloudStorageOutput{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	var err error
	if g.path, err = bloblang.NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if g.contentType, err = bloblang.NewField(conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content type expression: %v", err)
	}
	if g.contentEncoding, err = bloblang.NewField(conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content encoding expression: %v", err)
	}

	return g, nil
}

// ConnectWithContext attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *gcpCloudStorageOutput) ConnectWithContext(ctx context.Context) error {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var err error
	g.client, err = NewStorageClient(ctx)
	if err != nil {
		return err
	}

	g.log.Infof("Uploading message parts as objects to GCP Cloud Storage bucket: %v\n", g.conf.Bucket)
	return nil
}

// WriteWithContext attempts to write message contents to a target GCP Cloud
// Storage bucket as files.
func (g *gcpCloudStorageOutput) WriteWithContext(ctx context.Context, msg types.Message) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		metadata := map[string]string{}
		p.Metadata().Iter(func(k, v string) error {
			metadata[k] = v
			return nil
		})

		path := g.path.String(i, msg)
		_, err := client.Bucket(g.conf.Bucket).Object(path).Attrs(ctx)

		isMerge := false
		var tempPath string
		if err == storage.ErrObjectNotExist {
			tempPath = path
		} else {
			isMerge = true
			tempUUID, err := uuid.NewV4()
			if err != nil {
				return err
			}

			dir := filepath.Dir(path)
			tempFileName := fmt.Sprintf("%s.tmp", tempUUID.String())
			tempPath = filepath.Join(dir, tempFileName)
		}

		w := client.Bucket(g.conf.Bucket).Object(tempPath).NewWriter(ctx)

		w.ChunkSize = g.conf.ChunkSize
		w.ContentType = g.contentType.String(i, msg)
		w.ContentEncoding = g.contentEncoding.String(i, msg)
		w.Metadata = metadata
		_, err = w.Write(p.Get())
		if err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}

		if isMerge {
			err = g.appendToFile(path, tempPath, path)
			if err != nil {
				return err
			}
		}

		return err
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (g *gcpCloudStorageOutput) CloseAsync() {
	go func() {
		g.connMut.Lock()
		if g.client != nil {
			g.client.Close()
			g.client = nil
		}
		g.connMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (g *gcpCloudStorageOutput) WaitForClose(time.Duration) error {
	return nil
}

func (g *gcpCloudStorageOutput) appendToFile(source1, source2, dest string) error {
	client := g.client
	bucket := client.Bucket(g.conf.Bucket)
	src1 := bucket.Object(source1)
	src2 := bucket.Object(source2)
	dst := bucket.Object(dest)

	ctx := context.Background()
	_, err := dst.ComposerFrom(src1, src2).Run(ctx)
	if err != nil {
		return err
	}

	// Remove the temporary file used for the merge
	err = src2.Delete(ctx)
	if err != nil {
		g.log.Errorf("error deleting temp file in gcp: %w", err)
	}

	return nil
}
