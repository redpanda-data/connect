package gcp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/gofrs/uuid"
	"go.uber.org/multierr"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		g, err := newGCPCloudStorageOutput(nm, c.GCPCloudStorage)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("gcp_cloud_storage", c.GCPCloudStorage.MaxInFlight, g, nm)
		if err != nil {
			return nil, err
		}
		w = output.OnlySinglePayloads(w)
		return batcher.NewFromConfig(c.GCPCloudStorage.Batching, w, nm)
	}), docs.ComponentSpec{
		Name:       "gcp_cloud_storage",
		Type:       docs.TypeOutput,
		Status:     docs.StatusBeta,
		Version:    "3.43.0",
		Categories: []string{"Services", "GCP"},
		Summary: `
Sends message parts as objects to a Google Cloud Storage bucket. Each object is
uploaded with the path specified with the ` + "`path`" + ` field.`,
		Description: output.Description(true, true, `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/cloud/gcp).

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
			docs.FieldString("bucket", "The bucket to upload messages to."),
			docs.FieldString(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).IsInterpolated(),
			docs.FieldString("content_type", "The content type to set for each object.").IsInterpolated(),
			docs.FieldString("collision_mode", `Determines how file path collisions should be dealt with.`).
				HasDefault(`overwrite`).
				HasAnnotatedOptions(
					"overwrite", "Replace the existing file with the new one.",
					"append", "Append the message bytes to the original file.",
					"error-if-exists", "Return an error, this is the equivalent of a nack.",
					"ignore", "Do not modify the original file, the new data will be dropped.",
				).AtVersion("3.53.0"),
			docs.FieldString("content_encoding", "An optional content encoding to set for each object.").IsInterpolated().Advanced(),
			docs.FieldInt("chunk_size", "An optional chunk size which controls the maximum number of bytes of the object that the Writer will attempt to send to the server in a single request. If ChunkSize is set to zero, chunking will be disabled.").Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of message batches to have in flight at a given time. Increase this to improve throughput."),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewGCPCloudStorageConfig()),
	})
	if err != nil {
		panic(err)
	}
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
	mgr bundle.NewManagement,
	conf output.GCPCloudStorageConfig,
) (*gcpCloudStorageOutput, error) {
	g := &gcpCloudStorageOutput{
		conf:  conf,
		log:   mgr.Logger(),
		stats: mgr.Metrics(),
	}

	bEnv := mgr.BloblEnvironment()

	var err error
	if g.path, err = bEnv.NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if g.contentType, err = bEnv.NewField(conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content type expression: %v", err)
	}
	if g.contentEncoding, err = bEnv.NewField(conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content encoding expression: %v", err)
	}

	return g, nil
}

// Connect attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *gcpCloudStorageOutput) Connect(ctx context.Context) error {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var err error
	g.client, err = storage.NewClient(context.Background())
	if err != nil {
		return err
	}

	g.log.Infof("Uploading message parts as objects to GCP Cloud Storage bucket: %v\n", g.conf.Bucket)
	return nil
}

// WriteBatch attempts to write message contents to a target GCP Cloud
// Storage bucket as files.
func (g *gcpCloudStorageOutput) WriteBatch(ctx context.Context, msg message.Batch) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		metadata := map[string]string{}
		_ = p.MetaIterStr(func(k, v string) error {
			metadata[k] = v
			return nil
		})

		outputPath, err := g.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}
		if g.conf.CollisionMode != output.GCPCloudStorageOverwriteCollisionMode {
			_, err = client.Bucket(g.conf.Bucket).Object(outputPath).Attrs(ctx)
		}

		isMerge := false
		var tempPath string
		if errors.Is(err, storage.ErrObjectNotExist) || g.conf.CollisionMode == output.GCPCloudStorageOverwriteCollisionMode {
			tempPath = outputPath
		} else {
			isMerge = true

			if g.conf.CollisionMode == output.GCPCloudStorageErrorIfExistsCollisionMode {
				if err == nil {
					err = fmt.Errorf("file at path already exists: %s", outputPath)
				}
				return err
			} else if g.conf.CollisionMode == output.GCPCloudStorageIgnoreCollisionMode {
				return nil
			}

			tempUUID, err := uuid.NewV4()
			if err != nil {
				return err
			}

			dir := path.Dir(outputPath)
			tempFileName := fmt.Sprintf("%s.tmp", tempUUID.String())
			tempPath = path.Join(dir, tempFileName)

			g.log.Tracef("creating temporary file for the merge %q", tempPath)
		}

		src := client.Bucket(g.conf.Bucket).Object(tempPath)

		w := src.NewWriter(ctx)

		w.ChunkSize = g.conf.ChunkSize
		if w.ContentType, err = g.contentType.String(i, msg); err != nil {
			return fmt.Errorf("content type interpolation error: %w", err)
		}
		if w.ContentEncoding, err = g.contentEncoding.String(i, msg); err != nil {
			return fmt.Errorf("content encoding interpolation error: %w", err)
		}
		w.Metadata = metadata

		var errs error
		if _, werr := w.Write(p.AsBytes()); werr != nil {
			errs = multierr.Append(errs, werr)
		}

		if cerr := w.Close(); cerr != nil {
			errs = multierr.Append(errs, cerr)
		}

		if isMerge {
			defer g.removeTempFile(ctx, src)
		}

		if errs != nil {
			return errs
		}

		if isMerge {
			dst := client.Bucket(g.conf.Bucket).Object(outputPath)

			if aerr := g.appendToFile(ctx, src, dst); aerr != nil {
				return aerr
			}
		}

		return nil
	})
}

// Close begins cleaning up resources used by this reader asynchronously.
func (g *gcpCloudStorageOutput) Close(context.Context) error {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var err error
	if g.client != nil {
		err = g.client.Close()
		g.client = nil
	}
	return err
}

func (g *gcpCloudStorageOutput) appendToFile(ctx context.Context, src, dst *storage.ObjectHandle) error {
	_, err := dst.ComposerFrom(dst, src).Run(ctx)

	return err
}

func (g *gcpCloudStorageOutput) removeTempFile(ctx context.Context, src *storage.ObjectHandle) {
	// Remove the temporary file used for the merge
	g.log.Tracef("remove the temporary file used for the merge %q", src.ObjectName())
	if err := src.Delete(ctx); err != nil {
		g.log.Errorf("Failed to delete temporary file used for merging: %v", err)
	}
}
