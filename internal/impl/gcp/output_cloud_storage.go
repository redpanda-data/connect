package gcp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gofrs/uuid"
	"go.uber.org/multierr"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Cloud Storage Output Fields
	csoFieldBucket          = "bucket"
	csoFieldPath            = "path"
	csoFieldContentType     = "content_type"
	csoFieldContentEncoding = "content_encoding"
	csoFieldChunkSize       = "chunk_size"
	csoFieldMaxInFlight     = "max_in_flight"
	csoFieldBatching        = "batching"
	csoFieldCollisionMode   = "collision_mode"
	csoFieldTimeout         = "timeout"

	// GCPCloudStorageErrorIfExistsCollisionMode - error-if-exists.
	GCPCloudStorageErrorIfExistsCollisionMode = "error-if-exists"

	// GCPCloudStorageAppendCollisionMode - append.
	GCPCloudStorageAppendCollisionMode = "append"

	// GCPCloudStorageIgnoreCollisionMode - ignore.
	GCPCloudStorageIgnoreCollisionMode = "ignore"

	// GCPCloudStorageOverwriteCollisionMode - overwrite.
	GCPCloudStorageOverwriteCollisionMode = "overwrite"
)

type csoConfig struct {
	Bucket          string
	Path            *service.InterpolatedString
	ContentType     *service.InterpolatedString
	ContentEncoding *service.InterpolatedString
	ChunkSize       int
	CollisionMode   string
	Timeout         time.Duration
}

func csoConfigFromParsed(pConf *service.ParsedConfig) (conf csoConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(csoFieldBucket); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldInterpolatedString(csoFieldPath); err != nil {
		return
	}
	if conf.ContentType, err = pConf.FieldInterpolatedString(csoFieldContentType); err != nil {
		return
	}
	if conf.ContentEncoding, err = pConf.FieldInterpolatedString(csoFieldContentEncoding); err != nil {
		return
	}
	if conf.ChunkSize, err = pConf.FieldInt(csoFieldChunkSize); err != nil {
		return
	}
	if conf.CollisionMode, err = pConf.FieldString(csoFieldCollisionMode); err != nil {
		return
	}
	if conf.Timeout, err = pConf.FieldDuration(csoFieldTimeout); err != nil {
		return
	}
	return
}

func csoSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.43.0").
		Categories("Services", "GCP").
		Summary(`Sends message parts as objects to a Google Cloud Storage bucket. Each object is uploaded with the path specified with the `+"`path`"+` field.`).
		Description(output.Description(true, true, `
In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP services. You can find out more [in this document](/docs/guides/cloud/gcp).

### Batching

It's common to want to upload messages to Google Cloud Storage as batched archives, the easiest way to do this is to batch your messages at the output level and join the batch of messages with an `+"[`archive`](/docs/components/processors/archive)"+` and/or `+"[`compress`](/docs/components/processors/compress)"+` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents we could achieve that with the following config:

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

Alternatively, if we wished to upload JSON documents as a single large document containing an array of objects we can do that with:

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
`+"```"+``)).
		Fields(
			service.NewStringField(csoFieldBucket).
				Description("The bucket to upload messages to."),
			service.NewInterpolatedStringField(csoFieldPath).
				Description("The path of each message to upload.").
				Example(`${!count("files")}-${!timestamp_unix_nano()}.txt`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`),
			service.NewInterpolatedStringField(csoFieldContentType).
				Description("The content type to set for each object.").
				Default("application/octet-stream"),
			service.NewInterpolatedStringField(csoFieldContentEncoding).
				Description("An optional content encoding to set for each object.").
				Default("").
				Advanced(),
			service.NewStringAnnotatedEnumField(csoFieldCollisionMode, map[string]string{
				"overwrite":       "Replace the existing file with the new one.",
				"append":          "Append the message bytes to the original file.",
				"error-if-exists": "Return an error, this is the equivalent of a nack.",
				"ignore":          "Do not modify the original file, the new data will be dropped.",
			}).
				Description(`Determines how file path collisions should be dealt with.`).
				Version("3.53.0").
				Default(GCPCloudStorageOverwriteCollisionMode),
			service.NewIntField(csoFieldChunkSize).
				Description("An optional chunk size which controls the maximum number of bytes of the object that the Writer will attempt to send to the server in a single request. If ChunkSize is set to zero, chunking will be disabled.").
				Advanced().
				Default(16*1024*1024), // googleapi.DefaultUploadChunkSize
			service.NewDurationField(csoFieldTimeout).
				Description("The maximum period to wait on an upload before abandoning it and reattempting.").
				Example("1s").
				Example("500ms").
				Default("3s"),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of message batches to have in flight at a given time. Increase this to improve throughput."),
			service.NewBatchPolicyField(csoFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("gcp_cloud_storage", csoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(csoFieldBatching); err != nil {
				return
			}

			var pConf csoConfig
			if pConf, err = csoConfigFromParsed(conf); err != nil {
				return
			}

			out, err = newGCPCloudStorageOutput(pConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

// gcpCloudStorageOutput is a benthos writer.Type implementation that writes
// messages to a GCP Cloud Storage bucket.
type gcpCloudStorageOutput struct {
	conf csoConfig

	client  *storage.Client
	connMut sync.RWMutex

	log *service.Logger
}

// newGCPCloudStorageOutput creates a new GCP Cloud Storage bucket writer.Type.
func newGCPCloudStorageOutput(conf csoConfig, res *service.Resources) (*gcpCloudStorageOutput, error) {
	g := &gcpCloudStorageOutput{
		conf: conf,
		log:  res.Logger(),
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
	return nil
}

func (g *gcpCloudStorageOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(ctx, g.conf.Timeout)
	defer cancel()

	return batch.WalkWithBatchedErrors(func(i int, msg *service.Message) error {
		metadata := map[string]string{}
		_ = msg.MetaWalk(func(k, v string) error {
			metadata[k] = v
			return nil
		})

		outputPath, err := g.conf.Path.TryString(msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}
		if g.conf.CollisionMode != GCPCloudStorageOverwriteCollisionMode {
			_, err = client.Bucket(g.conf.Bucket).Object(outputPath).Attrs(ctx)
		}

		isMerge := false
		var tempPath string
		if errors.Is(err, storage.ErrObjectNotExist) || g.conf.CollisionMode == GCPCloudStorageOverwriteCollisionMode {
			tempPath = outputPath
		} else {
			isMerge = true

			if g.conf.CollisionMode == GCPCloudStorageErrorIfExistsCollisionMode {
				if err == nil {
					err = fmt.Errorf("file at path already exists: %s", outputPath)
				}
				return err
			} else if g.conf.CollisionMode == GCPCloudStorageIgnoreCollisionMode {
				return nil
			}

			tempUUID, err := uuid.NewV4()
			if err != nil {
				return err
			}

			dir := path.Dir(outputPath)
			tempFileName := tempUUID.String() + ".tmp"
			tempPath = path.Join(dir, tempFileName)

			g.log.Tracef("creating temporary file for the merge %q", tempPath)
		}

		src := client.Bucket(g.conf.Bucket).Object(tempPath)

		w := src.NewWriter(ctx)

		w.ChunkSize = g.conf.ChunkSize
		if w.ContentType, err = g.conf.ContentType.TryString(msg); err != nil {
			return fmt.Errorf("content type interpolation error: %w", err)
		}
		if w.ContentEncoding, err = g.conf.ContentEncoding.TryString(msg); err != nil {
			return fmt.Errorf("content encoding interpolation error: %w", err)
		}
		w.Metadata = metadata

		mBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		var errs error
		if _, werr := w.Write(mBytes); werr != nil {
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
