package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
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
		var rdr input.Async
		var err error
		rdr, err = newGCPCloudStorageInput(c.GCPCloudStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		// If we're not pulling events directly from a Pub/Sub subscription
		// then there's no concept of propagating nacks upstreams, therefore
		// wrap our reader within a preserver in order to retry indefinitely.
		if c.GCPCloudStorage.PubSub.Subscription == "" {
			rdr = input.NewAsyncPreserver(rdr)
		}
		return input.NewAsyncReader("gcp_cloud_storage", rdr, nm)
	}), docs.ComponentSpec{
		Name:       "gcp_cloud_storage",
		Type:       docs.TypeInput,
		Status:     docs.StatusBeta,
		Version:    "3.43.0",
		Categories: []string{"Services", "GCP"},
		Summary: `
Downloads objects within a Google Cloud Storage bucket, optionally filtered by a prefix, either by walking the items in the bucket or by streaming upload notifications in realtime.`,
		Description: `
## Streaming Objects on Upload with Pub/Sub

A common pattern for consuming GCS objects is to configure a bucket to emit upload notification events to a Pub/Sub topic with an associated subscription. A consumer then subscribes to this subscription and newly uploaded objects are then downloaded as notification events are published to the subscription. More information about this pattern and how to set it up can be found at: https://cloud.google.com/storage/docs/pubsub-notifications.

Benthos is able to follow this pattern when you configure ` + "`pubsub.project` and `pubsub.subscription`" + `, where it consumes events from Pub/Sub and only downloads object keys received within those events.

When using Pub/Sub please make sure you have sensible values for ` + "`pubsub.max_outstanding_messages`" + ` and also the acknowledgement deadline of the subscription itself. When Benthos consumes a GCS object the Pub/Sub message that triggered it is not acknowledged until the GCS object has been sent onwards. This ensures at-least-once crash resiliency, but also means that if the GCS object takes longer to process than the acknowledgement deadline of your subscription then the same objects might be processed multiple times.

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
			docs.FieldObject("pubsub", "Consume Pub/Sub messages in order to trigger key downloads.").WithChildren(
				docs.FieldString("project", "The project ID of the target subscription."),
				docs.FieldString("subscription", "The target subscription ID."),
				docs.FieldBool("sync", "Enable synchronous pull mode."),
				docs.FieldInt("max_outstanding_messages", "The maximum number of outstanding pending messages to be consumed at a given time."),
				docs.FieldInt("max_outstanding_bytes", "The maximum number of outstanding pending messages to be consumed measured in bytes."),
			),
			docs.FieldString("bucket", "The name of the bucket from which to download objects."),
			docs.FieldString("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldBool("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed.").Advanced(),
			docs.FieldInt("max_buffer", "The largest token size expected when consuming objects with a tokenised codec such as `lines`.").Advanced(),
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
	key    string
	bucket string
	ackFn  func(context.Context, error) error
}

func newGCPCloudStorageObjectTarget(key, bucket string, ackFn codec.ReaderAckFn) *gcpCloudStorageObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &gcpCloudStorageObjectTarget{key: key, bucket: bucket, ackFn: ackFn}
}

type gcpCloudStorageObjectTargetReader interface {
	Pop(ctx context.Context) (*gcpCloudStorageObjectTarget, error)
	Close(ctx context.Context) error
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
		staticKeys.pending = append(staticKeys.pending, newGCPCloudStorageObjectTarget(obj.Name, obj.Bucket, ackFn))
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
			r.pending = append(r.pending, newGCPCloudStorageObjectTarget(obj.Name, obj.Bucket, ackFn))
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

type pubsubTargetReader struct {
	conf          input.GCPCloudStorageConfig
	log           log.Modular
	msgsChan      chan *pubsub.Message
	storageClient *storage.Client

	nextRequest time.Time

	pending []*gcpCloudStorageObjectTarget
}

func newPubsubTargetReader(
	conf input.GCPCloudStorageConfig,
	log log.Modular,
	msgsChan chan *pubsub.Message,
	storageClient *storage.Client,
) *pubsubTargetReader {
	return &pubsubTargetReader{conf: conf, log: log, msgsChan: msgsChan, storageClient: storageClient, nextRequest: time.Time{}, pending: nil}
}

func (ps *pubsubTargetReader) Pop(ctx context.Context) (*gcpCloudStorageObjectTarget, error) {
	// we've got some events to pop off the stack, let's do one
	if len(ps.pending) > 0 {
		t := ps.pending[0]
		ps.pending = ps.pending[1:]
		return t, nil
	}

	// hang out until next pull interval or cancelled
	if !ps.nextRequest.IsZero() {
		if until := time.Until(ps.nextRequest); until > 0 {
			select {
			case <-time.After(until):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// go grab some events
	var err error
	if ps.pending, err = ps.readPubsubEvents(ctx); err != nil {
		return nil, err
	}
	// if no events waiting, trying again in 500ms
	// TODO: figure out why this is 500ms
	// TODO: figure out why this is supposd to return an error
	// I think the idea here is that if there isn't anything in last poll,
	// we don't hammer the API?
	if len(ps.pending) == 0 {
		ps.nextRequest = time.Now().Add(time.Millisecond * 500)
		return nil, component.ErrTimeout
	}

	ps.nextRequest = time.Time{}
	t := ps.pending[0]
	ps.pending = ps.pending[1:]
	return t, nil
}

func (ps *pubsubTargetReader) Close(ctx context.Context) error {
	// TODO: figure out -- I think this tries to NACK pending items?
	// ie. when you call ackFn with err != nil, it nacks msgs
	var err error
	for _, p := range ps.pending {
		if aerr := p.ackFn(ctx, errors.New("service shutting down")); aerr != nil {
			err = aerr
		}
	}
	return err
}

func (ps *pubsubTargetReader) parseObjectPath(pubsubMsgAttributes map[string]string) (*gcpCloudStorageObjectTarget, error) {
	eventType, ok := pubsubMsgAttributes["eventType"]
	if !ok {
		return nil, errors.New("pub/sub message missing eventType attribute")
	}
	if eventType != "OBJECT_FINALIZE" {
		return nil, errors.New("not an \"OBJECT_FINALIZE\" eventType")
	}
	// TODO: GCS FUSE likes to touch a file and write to it. This creates two OBJECT_FINALIZE
	// events, the first one with a 0 byte file. Figure out how to handle this.
	// I think maybe S3 input handles similar with sqs.delay_period feature?
	bucket, ok := pubsubMsgAttributes["bucketId"]
	if !ok {
		return nil, errors.New("pub/sub message missing bucketId attribute")
	}
	key, ok := pubsubMsgAttributes["objectId"]
	if !ok {
		return nil, errors.New("pub/sub message missing objectId attribute")
	}

	return &gcpCloudStorageObjectTarget{
		bucket: bucket,
		key:    key,
	}, nil
}

func (ps *pubsubTargetReader) readPubsubEvents(ctx context.Context) ([]*gcpCloudStorageObjectTarget, error) {
	var output []*pubsub.Message
	select {
	case gmsg := <-ps.msgsChan:
		output = append(output, gmsg)
		ps.log.Debugf("received msg on pub/sub channel = %v", gmsg)
	default:
	}

	// TODO: Currently just picks up a single message
	// maybe we should use a larger buffer channel, drop in and check the length of the channel
	// and drain it? Or maybe we select on the channel until maxmessages accumulated with
	// a default case of break?

	var pendingObjects []*gcpCloudStorageObjectTarget

	// Discard any Pub/Sub messages not associated with a target file.

	for _, pubsubMsg := range output {
		object, err := ps.parseObjectPath(pubsubMsg.Attributes)
		if err != nil {
			ps.log.Errorf("Pub/Sub extract key error: %v\n", err)
			continue
		}
		pendingObjects = append(pendingObjects, newGCPCloudStorageObjectTarget(
			object.key, object.bucket, deleteGCPCloudStorageObjectAckFn(
				ps.storageClient.Bucket(object.bucket), object.key, ps.conf.DeleteObjects,
				func(ctx context.Context, err error) (aerr error) {
					if err != nil {
						ps.log.Debugf("Abandoning Pub/Sub notification due to error: %v\n", err)
						aerr = ps.nackPubsubMessage(ctx, pubsubMsg)
					} else {
						aerr = ps.ackPubsubMessage(ctx, pubsubMsg)
					}
					return
				},
			)))
	}

	return pendingObjects, nil
}

func (ps *pubsubTargetReader) nackPubsubMessage(ctx context.Context, msg *pubsub.Message) error {

	msg.Nack()
	ps.log.Debugln("nack msg")
	return nil
}

func (ps *pubsubTargetReader) ackPubsubMessage(ctx context.Context, msg *pubsub.Message) error {

	msg.Ack()
	ps.log.Debugln("ack msg")
	return nil
}

//------------------------------------------------------------------------------

// gcpCloudStorage is a benthos reader.Type implementation that reads messages
// from a Google Cloud Storage bucket.
type gcpCloudStorageInput struct {
	conf input.GCPCloudStorageConfig

	objectScannerCtor codec.ReaderConstructor
	keyReader         gcpCloudStorageObjectTargetReader

	objectMut sync.Mutex
	object    *gcpCloudStoragePendingObject

	storageClient *storage.Client
	pubsubClient  *pubsub.Client

	subscription *pubsub.Subscription
	msgsChan     chan *pubsub.Message
	closeFunc    context.CancelFunc
	// TODO: additional mutex, or use the existing object one?
	subMut sync.Mutex

	log   log.Modular
	stats metrics.Type
}

// newGCPCloudStorageInput creates a new Google Cloud Storage input type.
func newGCPCloudStorageInput(conf input.GCPCloudStorageConfig, log log.Modular, stats metrics.Type) (*gcpCloudStorageInput, error) {
	if conf.Bucket == "" && conf.PubSub.Subscription == "" {
		return nil, errors.New("either a bucket or a pubsub.subscription must be specified")
	}
	if conf.Prefix != "" && conf.PubSub.Subscription != "" {
		return nil, errors.New("cannot specify both a prefix and pubsub.subscription")
	}
	if conf.PubSub.Project == "" && conf.PubSub.Subscription != "" {
		return nil, errors.New("pubsub.project must be specified with pubsub.subscription")
	}

	readerConfig := codec.NewReaderConfig()
	readerConfig.MaxScanTokenSize = conf.MaxBuffer

	objectScannerCtor, err := codec.GetReader(conf.Codec, readerConfig)
	if err != nil {
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

func (g *gcpCloudStorageInput) getTargetReader(ctx context.Context) (gcpCloudStorageObjectTargetReader, error) {
	if g.pubsubClient != nil {
		return newPubsubTargetReader(g.conf, g.log, g.msgsChan, g.storageClient), nil
	}
	return newGCPCloudStorageTargetReader(ctx, g.conf, g.log, g.storageClient.Bucket(g.conf.Bucket))
}

// Connect attempts to establish a connection to the target Google
// Cloud Storage bucket and any relevant Pub/Sub subscription used to
// traverse the objects.
func (g *gcpCloudStorageInput) Connect(ctx context.Context) error {
	var err error
	// TODO: understand why is this a new context in original code?
	g.storageClient, err = storage.NewClient(context.Background())
	if err != nil {
		return err
	}
	if g.conf.PubSub.Subscription != "" {
		// TODO: Again, should this be a new context or should we use existing?
		g.pubsubClient, err = pubsub.NewClient(context.Background(), g.conf.PubSub.Project)
		if err != nil {
			return err
		}

		g.subMut.Lock()
		defer g.subMut.Unlock()
		if g.subscription != nil {
			return nil
		}

		sub := g.pubsubClient.Subscription(g.conf.PubSub.Subscription)
		sub.ReceiveSettings.MaxOutstandingMessages = g.conf.PubSub.MaxOutstandingMessages
		sub.ReceiveSettings.MaxOutstandingBytes = g.conf.PubSub.MaxOutstandingBytes
		sub.ReceiveSettings.Synchronous = g.conf.PubSub.Sync

		// TODO: why create new context? should we just use ctx?
		subCtx, cancel := context.WithCancel(context.Background())
		// TODO: should the channel buffer size be configurable?
		// TODO: Does channel size create natural back pressure on Google side? In other
		// words, the call-back we pass to sub.Receive would block on channel send
		// until there is buffer available. Maybe it eventually results in disconnects by
		// Google?
		// Probably not a good idea? Maybe this should be operated in synchronous mode instead?
		// sub.ReceiveSettings.Synchronous = true
		// sub.ReceiveSettings.MaxOutstandingMessages = 10
		msgsChan := make(chan *pubsub.Message, 1)

		g.subscription = sub
		g.msgsChan = msgsChan
		g.closeFunc = cancel

		// launch goroutine to receive streaming messages from pub/sub
		go func() {
			g.log.Debugln("entering pub/sub receiver goroutine")
			rerr := sub.Receive(subCtx, func(ctx context.Context, m *pubsub.Message) {
				g.log.Debugf("received pub/sub msg inside receiver callback. m = %v\n", m)
				select {
				case msgsChan <- m:
					g.log.Debugf("sending pub/sub msg into message channel inside receiver goroutine. m = %v\n", m)
				case <-ctx.Done():
					g.log.Debugf("cancel received, abandoning pub/sub message inside receiver goroutine. m = %v\n", m)
					if m != nil {
						m.Nack()
					}
				}
				g.log.Debugln("exiting pub/sub receiver callback")
			})
			if rerr != nil && rerr != context.Canceled {
				g.log.Errorf("Subscription error: %v\n", rerr)
			}
			g.subMut.Lock()
			g.subscription = nil
			close(g.msgsChan)
			g.msgsChan = nil
			g.closeFunc = nil
			g.subMut.Unlock()
			g.log.Debugf("exiting pub/sub receiver goroutine")
		}()
	}
	if g.keyReader, err = g.getTargetReader(ctx); err != nil {
		g.pubsubClient = nil
		g.storageClient = nil
		return err
	}

	if g.conf.PubSub.Subscription == "" {
		g.log.Infof("Downloading GCS objects from bucket: %s\n", g.conf.Bucket)
	} else {
		g.log.Infof("Downloading GCS objects found in messages from Pub/Sub subscription: %s\n", g.conf.PubSub.Subscription)
	}
	return nil
}

func (g *gcpCloudStorageInput) getObjectTarget(ctx context.Context) (*gcpCloudStoragePendingObject, error) {
	if g.object != nil {
		return g.object, nil
	}

	target, err := g.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: make sure the gcs object lister stores the bucket in the new target bucket attribute rather than
	// assuming we'd get it from config right here (previous behavior)
	objReference := g.storageClient.Bucket(target.bucket).Object(target.key)

	// TODO: why are we acknowledging every target that throws an error?
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
		// TODO: EOF check copied from input_s3 logic... keep?
		// Warning: NEVER return io.EOF from a scanner constructor, as this will
		// falsely indicate that we've reached the end of our list of object
		// targets when running a Pub/Sub feed.
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("encountered an empty file for key '%v'", target.key)
		}
		_ = target.ackFn(ctx, err)
		return nil, err
	}
	// TODO: Why aren't we using the object mutex around this?
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

	if err == nil && g.storageClient != nil {
		err = g.storageClient.Close()
		g.storageClient = nil
	}

	if err == nil && g.pubsubClient != nil {
		err = g.pubsubClient.Close()
		g.pubsubClient = nil
	}
	return
}
