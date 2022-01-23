package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"google.golang.org/api/iterator"
)

func init() {
	bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c input.Config, nm bundle.NewManagement) (input.Type, error) {
		r, err := newGCPCloudStorageInput(c.GCPCloudStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader(
			input.TypeGCPCloudStorage, true,
			reader.NewAsyncPreserver(r),
			nm.Logger(), nm.Metrics(),
		)
	}), docs.ComponentSpec{
		Name:    input.TypeGCPCloudStorage,
		Type:    docs.TypeInput,
		Status:  docs.StatusBeta,
		Version: "3.43.0",
		Categories: []string{
			string(input.CategoryServices),
			string(input.CategoryGCP),
		},
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

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/cloud/gcp).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon("bucket", "The name of the bucket from which to download objects."),
			docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed."),
		).ChildDefaultAndTypesFromStruct(input.NewGCPCloudStorageConfig()),
	})
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
		if err == iterator.Done {
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
			if err == iterator.Done {
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

// ConnectWithContext attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *gcpCloudStorageInput) ConnectWithContext(ctx context.Context) error {
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

func gcpCloudStorageMsgFromParts(p *gcpCloudStoragePendingObject, parts []types.Part) types.Message {
	msg := message.New(nil)
	msg.Append(parts...)
	msg.Iter(func(_ int, part types.Part) error {
		meta := part.Metadata()

		meta.Set("gcs_key", p.target.key)
		meta.Set("gcs_bucket", p.obj.Bucket)
		meta.Set("gcs_last_modified", p.obj.Updated.Format(time.RFC3339))
		meta.Set("gcs_last_modified_unix", strconv.FormatInt(p.obj.Updated.Unix(), 10))
		meta.Set("gcs_content_type", p.obj.ContentType)
		meta.Set("gcs_content_encoding", p.obj.ContentEncoding)

		for k, v := range p.obj.Metadata {
			meta.Set(k, v)
		}
		return nil
	})

	return msg
}

// ReadWithContext attempts to read a new message from the target Google Cloud
// Storage bucket.
func (g *gcpCloudStorageInput) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	g.objectMut.Lock()
	defer g.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = types.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = types.ErrTimeout
		}
	}()

	var object *gcpCloudStoragePendingObject
	if object, err = g.getObjectTarget(ctx); err != nil {
		return
	}

	var parts []types.Part
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

	return gcpCloudStorageMsgFromParts(object, parts), func(rctx context.Context, res types.Response) error {
		return scnAckFn(rctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (g *gcpCloudStorageInput) CloseAsync() {
	go func() {
		g.objectMut.Lock()
		if g.object != nil {
			g.object.scanner.Close(context.Background())
			g.object = nil
		}

		if g.client != nil {
			g.client.Close()
			g.client = nil
		}

		g.objectMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (g *gcpCloudStorageInput) WaitForClose(time.Duration) error {
	return nil
}
