package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/codec"
)

const (
	// Cloud Storage Input Fields
	csiFieldBucket        = "bucket"
	csiFieldPrefix        = "prefix"
	csiFieldDeleteObjects = "delete_objects"
)

type csiConfig struct {
	Bucket        string
	Prefix        string
	DeleteObjects bool
	Codec         codec.DeprecatedFallbackCodec
}

func csiConfigFromParsed(pConf *service.ParsedConfig) (conf csiConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(csiFieldBucket); err != nil {
		return
	}
	if conf.Prefix, err = pConf.FieldString(csiFieldPrefix); err != nil {
		return
	}
	if conf.Codec, err = codec.DeprecatedCodecFromParsed(pConf); err != nil {
		return
	}
	if conf.DeleteObjects, err = pConf.FieldBool(csiFieldDeleteObjects); err != nil {
		return
	}
	return
}

func csiSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.43.0").
		Categories("Services", "GCP").
		Summary(`Downloads objects within a Google Cloud Storage bucket, optionally filtered by a prefix.`).
		Description(`
== Metadata

This input adds the following metadata fields to each message:

`+"```"+`
- gcs_key
- gcs_bucket
- gcs_last_modified
- gcs_last_modified_unix
- gcs_content_type
- gcs_content_encoding
- All user defined metadata
`+"```"+`

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].

=== Credentials

By default Benthos will use a shared credentials file when connecting to GCP services. You can find out more in xref:guides:cloud/gcp.adoc[].`).
		Fields(
			service.NewStringField(csiFieldBucket).
				Description("The name of the bucket from which to download objects."),
			service.NewStringField(csiFieldPrefix).
				Description("An optional path prefix, if set only objects with the prefix are consumed.").
				Default(""),
		).
		Fields(codec.DeprecatedCodecFields("to_the_end")...).
		Fields(
			service.NewBoolField(csiFieldDeleteObjects).
				Description("Whether to delete downloaded objects from the bucket once they are processed.").
				Advanced().
				Default(false),
		)
}

func init() {
	err := service.RegisterBatchInput("gcp_cloud_storage", csiSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			conf, err := csiConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			var rdr service.BatchInput
			if rdr, err = newGCPCloudStorageInput(conf, res); err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(rdr), nil
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

func newGCPCloudStorageObjectTarget(key string, ackFn service.AckFunc) *gcpCloudStorageObjectTarget {
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
	prev service.AckFunc,
) service.AckFunc {
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
	scanner   codec.DeprecatedFallbackStream
}

type gcpCloudStorageTargetReader struct {
	pending    []*gcpCloudStorageObjectTarget
	bucket     *storage.BucketHandle
	conf       csiConfig
	startAfter *storage.ObjectIterator
}

func newGCPCloudStorageTargetReader(
	ctx context.Context,
	conf csiConfig,
	log *service.Logger,
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
	conf csiConfig

	objectScannerCtor codec.DeprecatedFallbackCodec
	keyReader         *gcpCloudStorageTargetReader

	objectMut sync.Mutex
	object    *gcpCloudStoragePendingObject

	client *storage.Client

	log *service.Logger
}

// newGCPCloudStorageInput creates a new Google Cloud Storage input type.
func newGCPCloudStorageInput(conf csiConfig, res *service.Resources) (*gcpCloudStorageInput, error) {
	g := &gcpCloudStorageInput{
		conf:              conf,
		objectScannerCtor: conf.Codec,
		log:               res.Logger(),
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
	details := service.NewScannerSourceDetails()
	details.SetName(target.key)
	if object.scanner, err = g.objectScannerCtor.Create(objReader, target.ackFn, details); err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	g.object = object
	return object, nil
}

func gcpCloudStorageMetaToParts(p *gcpCloudStoragePendingObject, parts service.MessageBatch) {
	for _, part := range parts {
		part.MetaSetMut("gcs_key", p.target.key)
		part.MetaSetMut("gcs_bucket", p.obj.Bucket)
		part.MetaSetMut("gcs_last_modified", p.obj.Updated.Format(time.RFC3339))
		part.MetaSetMut("gcs_last_modified_unix", p.obj.Updated.Unix())
		part.MetaSetMut("gcs_content_type", p.obj.ContentType)
		part.MetaSetMut("gcs_content_encoding", p.obj.ContentEncoding)

		for k, v := range p.obj.Metadata {
			part.MetaSetMut(k, v)
		}
	}
}

// ReadBatch attempts to read a new message from the target Google Cloud
// Storage bucket.
func (g *gcpCloudStorageInput) ReadBatch(ctx context.Context) (msg service.MessageBatch, ackFn service.AckFunc, err error) {
	g.objectMut.Lock()
	defer g.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = service.ErrEndOfInput
		}
	}()

	var object *gcpCloudStoragePendingObject
	if object, err = g.getObjectTarget(ctx); err != nil {
		return
	}

	var parts service.MessageBatch
	var scnAckFn service.AckFunc

	for {
		if parts, scnAckFn, err = object.scanner.NextBatch(ctx); err == nil {
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

	gcpCloudStorageMetaToParts(object, parts)

	return parts, func(rctx context.Context, res error) error {
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
