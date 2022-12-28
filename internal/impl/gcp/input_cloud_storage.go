package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		r, err := newGCPCloudStorageInput(c.GCPCloudStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("gcp_cloud_storage", input.NewAsyncPreserver(r), nm)
	}), docs.ComponentSpec{
		Name:       "gcp_cloud_storage",
		Type:       docs.TypeInput,
		Status:     docs.StatusBeta,
		Version:    "3.43.0",
		Categories: []string{"Services", "GCP"},
		Summary: `
Downloads objects within a Google Cloud Storage bucket, optionally filtered by a prefix.`,
		Description: `
## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- gcs_key
- gcs_bucket
- gcs_last_modified
- gcs_last_modified_unix
- gcs_content_type
- gcs_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/cloud/gcp).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("bucket", "The name of the bucket from which to download objects."),
			docs.FieldString("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldBool("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewGCPCloudStorageConfig()),
	})
	if err != nil {
		panic(err)
	}
}

const (
	maxGCPCloudStorageListObjectsResults = 100
)

type gcpCloudStorageObjectTarget struct {
	key   string
	ackFn func(context.Context, error) error
}

func newGCPCloudStorageObjectTarget(key string, ackFn codec.ReaderAckFn) *gcpCloudStorageObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &gcpCloudStorageObjectTarget{key: key, ackFn: ackFn}
}

//------------------------------------------------------------------------------

func deleteGCPCloudStorageObjectAckFn(
	bucket *storage.BucketHandle,
	key string,
	del bool,
	prev codec.ReaderAckFn,
) codec.ReaderAckFn {
	return func(ctx context.Context, err error) error {
		if prev != nil {
			if aerr := prev(ctx, err); aerr != nil {
				return aerr
			}
		}
		if !del || err != nil {
			return nil
		}

		return bucket.Object(key).Delete(ctx)
	}
}

//------------------------------------------------------------------------------

type gcpCloudStoragePendingObject struct {
	target    *gcpCloudStorageObjectTarget
	obj       *storage.ObjectAttrs
	extracted int
	scanner   codec.Reader
}

type gcpCloudStorageTargetReader struct {
	pending    []*gcpCloudStorageObjectTarget
	bucket     *storage.BucketHandle
	conf       input.GCPCloudStorageConfig
	startAfter *storage.ObjectIterator
}

func newGCPCloudStorageTargetReader(
	ctx context.Context,
	conf input.GCPCloudStorageConfig,
	log log.Modular,
	bucket *storage.BucketHandle,
) (*gcpCloudStorageTargetReader, error) {
	staticKeys := gcpCloudStorageTargetReader{
		bucket: bucket,
		conf:   conf,
	}

	it := bucket.Objects(ctx, &storage.Query{Prefix: conf.Prefix})
	for count := 0; count < maxGCPCloudStorageListObjectsResults; count++ {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}

		ackFn := deleteGCPCloudStorageObjectAckFn(bucket, obj.Name, conf.DeleteObjects, nil)
		staticKeys.pending = append(staticKeys.pending, newGCPCloudStorageObjectTarget(obj.Name, ackFn))
	}

	if len(staticKeys.pending) > 0 {
		staticKeys.startAfter = it
	}

	return &staticKeys, nil
}

func (r *gcpCloudStorageTargetReader) Pop(ctx context.Context) (*gcpCloudStorageObjectTarget, error) {
	if len(r.pending) == 0 && r.startAfter != nil {
		r.pending = nil

		for count := 0; count < maxGCPCloudStorageListObjectsResults; count++ {
			obj, err := r.startAfter.Next()
			if errors.Is(err, iterator.Done) {
				break
			} else if err != nil {
				return nil, fmt.Errorf("failed to list objects: %v", err)
			}

			ackFn := deleteGCPCloudStorageObjectAckFn(r.bucket, obj.Name, r.conf.DeleteObjects, nil)
			r.pending = append(r.pending, newGCPCloudStorageObjectTarget(obj.Name, ackFn))
		}
	}
	if len(r.pending) == 0 {
		return nil, io.EOF
	}
	obj := r.pending[0]
	r.pending = r.pending[1:]
	return obj, nil
}

func (r gcpCloudStorageTargetReader) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

// gcpCloudStorage is a benthos reader.Type implementation that reads messages
// from a Google Cloud Storage bucket.
type gcpCloudStorageInput struct {
	conf input.GCPCloudStorageConfig

	objectScannerCtor codec.ReaderConstructor
	keyReader         *gcpCloudStorageTargetReader

	objectMut sync.Mutex
	object    *gcpCloudStoragePendingObject

	client *storage.Client

	log   log.Modular
	stats metrics.Type
}

// newGCPCloudStorageInput creates a new Google Cloud Storage input type.
func newGCPCloudStorageInput(conf input.GCPCloudStorageConfig, log log.Modular, stats metrics.Type) (*gcpCloudStorageInput, error) {
	var objectScannerCtor codec.ReaderConstructor
	var err error
	if objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, fmt.Errorf("invalid google cloud storage codec: %v", err)
	}

	g := &gcpCloudStorageInput{
		conf:              conf,
		objectScannerCtor: objectScannerCtor,
		log:               log,
		stats:             stats,
	}

	return g, nil
}

// Connect attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *gcpCloudStorageInput) Connect(ctx context.Context) error {
	var err error
	g.client, err = storage.NewClient(context.Background())
	if err != nil {
		return err
	}

	g.keyReader, err = newGCPCloudStorageTargetReader(ctx, g.conf, g.log, g.client.Bucket(g.conf.Bucket))
	return err
}

func (g *gcpCloudStorageInput) getObjectTarget(ctx context.Context) (*gcpCloudStoragePendingObject, error) {
	if g.object != nil {
		return g.object, nil
	}

	target, err := g.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	objReference := g.client.Bucket(g.conf.Bucket).Object(target.key)

	objAttributes, err := objReference.Attrs(ctx)
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	objReader, err := objReference.NewReader(context.Background())
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	object := &gcpCloudStoragePendingObject{
		target: target,
		obj:    objAttributes,
	}
	if object.scanner, err = g.objectScannerCtor(target.key, objReader, target.ackFn); err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	g.object = object
	return object, nil
}

func gcpCloudStorageMsgFromParts(p *gcpCloudStoragePendingObject, parts []*message.Part) message.Batch {
	msg := message.Batch(parts)
	_ = msg.Iter(func(_ int, part *message.Part) error {
		part.MetaSetMut("gcs_key", p.target.key)
		part.MetaSetMut("gcs_bucket", p.obj.Bucket)
		part.MetaSetMut("gcs_last_modified", p.obj.Updated.Format(time.RFC3339))
		part.MetaSetMut("gcs_last_modified_unix", p.obj.Updated.Unix())
		part.MetaSetMut("gcs_content_type", p.obj.ContentType)
		part.MetaSetMut("gcs_content_encoding", p.obj.ContentEncoding)

		for k, v := range p.obj.Metadata {
			part.MetaSetMut(k, v)
		}
		return nil
	})

	return msg
}

// ReadBatch attempts to read a new message from the target Google Cloud
// Storage bucket.
func (g *gcpCloudStorageInput) ReadBatch(ctx context.Context) (msg message.Batch, ackFn input.AsyncAckFn, err error) {
	g.objectMut.Lock()
	defer g.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = component.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = component.ErrTimeout
		}
	}()

	var object *gcpCloudStoragePendingObject
	if object, err = g.getObjectTarget(ctx); err != nil {
		return
	}

	var parts []*message.Part
	var scnAckFn codec.ReaderAckFn

	for {
		if parts, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			object.extracted++
			break
		}
		g.object = nil
		if err != io.EOF {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			g.log.Warnf("Failed to close object scanner cleanly: %v\n", err)
		}
		if object.extracted == 0 {
			g.log.Debugf("Extracted zero messages from key %v\n", object.target.key)
		}
		if object, err = g.getObjectTarget(ctx); err != nil {
			return
		}
	}

	return gcpCloudStorageMsgFromParts(object, parts), func(rctx context.Context, res error) error {
		return scnAckFn(rctx, res)
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (g *gcpCloudStorageInput) Close(ctx context.Context) (err error) {
	g.objectMut.Lock()
	defer g.objectMut.Unlock()

	if g.object != nil {
		err = g.object.scanner.Close(ctx)
		g.object = nil
	}

	if err == nil && g.client != nil {
		err = g.client.Close()
		g.client = nil
	}
	return
}
