package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		var rdr input.Async
		var err error
		if rdr, err = newAmazonS3Reader(conf.AWSS3, nm); err != nil {
			return nil, err
		}
		// If we're not pulling events directly from an SQS queue then
		// there's no concept of propagating nacks upstream, therefore wrap
		// our reader within a preserver in order to retry indefinitely.
		if conf.AWSS3.SQS.URL == "" {
			rdr = input.NewAsyncPreserver(rdr)
		}
		return input.NewAsyncReader("aws_s3", rdr, nm)
	}), docs.ComponentSpec{
		Name:   "aws_s3",
		Status: docs.StatusStable,
		Summary: `
Downloads objects within an Amazon S3 bucket, optionally filtered by a prefix, either by walking the items in the bucket or by streaming upload notifications in realtime.`,
		Description: `
## Streaming Objects on Upload with SQS

A common pattern for consuming S3 objects is to emit upload notification events from the bucket either directly to an SQS queue, or to an SNS topic that is consumed by an SQS queue, and then have your consumer listen for events which prompt it to download the newly uploaded objects. More information about this pattern and how to set it up can be found at: https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html.

Benthos is able to follow this pattern when you configure an ` + "`sqs.url`" + `, where it consumes events from SQS and only downloads object keys received within those events. In order for this to work Benthos needs to know where within the event the key and bucket names can be found, specified as [dot paths](/docs/configuration/field_paths) with the fields ` + "`sqs.key_path` and `sqs.bucket_path`" + `. The default values for these fields should already be correct when following the guide above.

If your notification events are being routed to SQS via an SNS topic then the events will be enveloped by SNS, in which case you also need to specify the field ` + "`sqs.envelope_path`" + `, which in the case of SNS to SQS will usually be ` + "`Message`" + `.

When using SQS please make sure you have sensible values for ` + "`sqs.max_messages`" + ` and also the visibility timeout of the queue itself. When Benthos consumes an S3 object the SQS message that triggered it is not deleted until the S3 object has been sent onwards. This ensures at-least-once crash resiliency, but also means that if the S3 object takes longer to process than the visibility timeout of your queue then the same objects might be processed multiple times.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- s3_key
- s3_bucket
- s3_last_modified_unix
- s3_last_modified (RFC3339)
- s3_content_type
- s3_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries). Note that user defined metadata is case insensitive within AWS, and it is likely that the keys will be received in a capitalized form, if you wish to make them consistent you can map all metadata keys to lower or uppercase using a Bloblang mapping such as ` + "`meta = meta().map_each_key(key -> key.lowercase())`" + `.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("bucket", "The bucket to consume from. If the field `sqs.url` is specified this field is optional."),
			docs.FieldString("prefix", "An optional path prefix, if set only objects with the prefix are consumed when walking a bucket."),
		).WithChildren(sess.FieldSpecs()...).WithChildren(
			docs.FieldBool("force_path_style_urls", "Forces the client API to use path style URLs for downloading keys, which is often required when connecting to custom endpoints.").Advanced(),
			docs.FieldBool("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed.").Advanced(),
			codec.ReaderDocs,
			docs.FieldInt("max_buffer", "The largest token size expected when consuming objects with a tokenised codec such as `lines`.").Advanced(),
			docs.FieldObject("sqs", "Consume SQS messages in order to trigger key downloads.").WithChildren(
				docs.FieldURL("url", "An optional SQS URL to connect to. When specified this queue will control which objects are downloaded."),
				docs.FieldString("endpoint", "A custom endpoint to use when connecting to SQS.").Advanced(),
				docs.FieldString("key_path", "A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages."),
				docs.FieldString("bucket_path", "A [dot path](/docs/configuration/field_paths) whereby the bucket name can be found in SQS messages."),
				docs.FieldString("envelope_path", "A [dot path](/docs/configuration/field_paths) of a field to extract an enveloped JSON payload for further extracting the key and bucket from SQS messages. This is specifically useful when subscribing an SQS queue to an SNS topic that receives bucket events.", "Message"),
				docs.FieldString(
					"delay_period",
					"An optional period of time to wait from when a notification was originally sent to when the target key download is attempted.",
					"10s", "5m",
				).Advanced(),
				docs.FieldInt("max_messages", "The maximum number of SQS messages to consume from each request.").Advanced(),
			),
		).ChildDefaultAndTypesFromStruct(input.NewAWSS3Config()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

//------------------------------------------------------------------------------

type s3ObjectTarget struct {
	key            string
	bucket         string
	notificationAt time.Time

	ackFn func(context.Context, error) error
}

func newS3ObjectTarget(key, bucket string, notificationAt time.Time, ackFn codec.ReaderAckFn) *s3ObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &s3ObjectTarget{key: key, bucket: bucket, notificationAt: notificationAt, ackFn: ackFn}
}

type s3ObjectTargetReader interface {
	Pop(ctx context.Context) (*s3ObjectTarget, error)
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

func deleteS3ObjectAckFn(
	s3Client *s3.S3,
	bucket, key string,
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
		_, aerr := s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return aerr
	}
}

//------------------------------------------------------------------------------

type staticTargetReader struct {
	pending    []*s3ObjectTarget
	s3         *s3.S3
	conf       input.AWSS3Config
	startAfter *string
}

func newStaticTargetReader(
	ctx context.Context,
	conf input.AWSS3Config,
	log log.Modular,
	s3Client *s3.S3,
) (*staticTargetReader, error) {
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(conf.Bucket),
		MaxKeys: aws.Int64(100),
	}
	if len(conf.Prefix) > 0 {
		listInput.Prefix = aws.String(conf.Prefix)
	}
	output, err := s3Client.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %v", err)
	}
	staticKeys := staticTargetReader{
		s3:   s3Client,
		conf: conf,
	}
	for _, obj := range output.Contents {
		ackFn := deleteS3ObjectAckFn(s3Client, conf.Bucket, *obj.Key, conf.DeleteObjects, nil)
		staticKeys.pending = append(staticKeys.pending, newS3ObjectTarget(*obj.Key, conf.Bucket, time.Time{}, ackFn))
	}
	if len(output.Contents) > 0 {
		staticKeys.startAfter = output.Contents[len(output.Contents)-1].Key
	}
	return &staticKeys, nil
}

func (s *staticTargetReader) Pop(ctx context.Context) (*s3ObjectTarget, error) {
	if len(s.pending) == 0 && s.startAfter != nil {
		s.pending = nil
		listInput := &s3.ListObjectsV2Input{
			Bucket:     aws.String(s.conf.Bucket),
			MaxKeys:    aws.Int64(100),
			StartAfter: s.startAfter,
		}
		if len(s.conf.Prefix) > 0 {
			listInput.Prefix = aws.String(s.conf.Prefix)
		}
		output, err := s.s3.ListObjectsV2WithContext(ctx, listInput)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}
		for _, obj := range output.Contents {
			ackFn := deleteS3ObjectAckFn(s.s3, s.conf.Bucket, *obj.Key, s.conf.DeleteObjects, nil)
			s.pending = append(s.pending, newS3ObjectTarget(*obj.Key, s.conf.Bucket, time.Time{}, ackFn))
		}
		if len(output.Contents) > 0 {
			s.startAfter = output.Contents[len(output.Contents)-1].Key
		}
	}
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	obj := s.pending[0]
	s.pending = s.pending[1:]
	return obj, nil
}

func (s staticTargetReader) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type sqsTargetReader struct {
	conf input.AWSS3Config
	log  log.Modular
	sqs  *sqs.SQS
	s3   *s3.S3

	nextRequest time.Time

	pending []*s3ObjectTarget
}

func newSQSTargetReader(
	conf input.AWSS3Config,
	log log.Modular,
	s3 *s3.S3,
	sqs *sqs.SQS,
) *sqsTargetReader {
	return &sqsTargetReader{conf: conf, log: log, sqs: sqs, s3: s3, nextRequest: time.Time{}, pending: nil}
}

func (s *sqsTargetReader) Pop(ctx context.Context) (*s3ObjectTarget, error) {
	if len(s.pending) > 0 {
		t := s.pending[0]
		s.pending = s.pending[1:]
		return t, nil
	}

	if !s.nextRequest.IsZero() {
		if until := time.Until(s.nextRequest); until > 0 {
			select {
			case <-time.After(until):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	var err error
	if s.pending, err = s.readSQSEvents(ctx); err != nil {
		return nil, err
	}
	if len(s.pending) == 0 {
		s.nextRequest = time.Now().Add(time.Millisecond * 500)
		return nil, component.ErrTimeout
	}
	s.nextRequest = time.Time{}
	t := s.pending[0]
	s.pending = s.pending[1:]
	return t, nil
}

func (s *sqsTargetReader) Close(ctx context.Context) error {
	var err error
	for _, p := range s.pending {
		if aerr := p.ackFn(ctx, errors.New("service shutting down")); aerr != nil {
			err = aerr
		}
	}
	return err
}

func digStrsFromSlices(slice []any) []string {
	var strs []string
	for _, v := range slice {
		switch t := v.(type) {
		case []any:
			strs = append(strs, digStrsFromSlices(t)...)
		case string:
			strs = append(strs, t)
		}
	}
	return strs
}

func (s *sqsTargetReader) parseObjectPaths(sqsMsg *string) ([]s3ObjectTarget, error) {
	gObj, err := gabs.ParseJSON([]byte(*sqsMsg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQS message: %v", err)
	}

	if len(s.conf.SQS.EnvelopePath) > 0 {
		d := gObj.Path(s.conf.SQS.EnvelopePath).Data()
		if str, ok := d.(string); ok {
			if gObj, err = gabs.ParseJSON([]byte(str)); err != nil {
				return nil, fmt.Errorf("failed to parse enveloped message: %v", err)
			}
		} else {
			return nil, fmt.Errorf("expected string at envelope path, found %T", d)
		}
	}

	var keys []string
	var buckets []string

	switch t := gObj.Path(s.conf.SQS.KeyPath).Data().(type) {
	case string:
		keys = []string{t}
	case []any:
		keys = digStrsFromSlices(t)
	}
	if len(s.conf.SQS.BucketPath) > 0 {
		switch t := gObj.Path(s.conf.SQS.BucketPath).Data().(type) {
		case string:
			buckets = []string{t}
		case []any:
			buckets = digStrsFromSlices(t)
		}
	}

	objects := make([]s3ObjectTarget, 0, len(keys))
	for i, key := range keys {
		if key, err = url.QueryUnescape(key); err != nil {
			return nil, fmt.Errorf("failed to parse key from SQS message: %v", err)
		}
		bucket := s.conf.Bucket
		if len(buckets) > i {
			bucket = buckets[i]
		}
		if bucket == "" {
			return nil, errors.New("required bucket was not found in SQS message")
		}
		objects = append(objects, s3ObjectTarget{
			key:    key,
			bucket: bucket,
		})
	}

	return objects, nil
}

func (s *sqsTargetReader) readSQSEvents(ctx context.Context) ([]*s3ObjectTarget, error) {
	var dudMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	addDudFn := func(m *sqs.Message) {
		dudMessageHandles = append(dudMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	output, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.conf.SQS.URL),
		MaxNumberOfMessages: aws.Int64(s.conf.SQS.MaxMessages),
		AttributeNames: []*string{
			aws.String("SentTimestamp"),
		},
	})
	if err != nil {
		return nil, err
	}

	var pendingObjects []*s3ObjectTarget

	for _, sqsMsg := range output.Messages {
		sqsMsg := sqsMsg

		var notificationAt time.Time
		if rcvd, ok := sqsMsg.Attributes["SentTimestamp"]; ok && rcvd != nil {
			if millis, _ := strconv.Atoi(*rcvd); millis > 0 {
				notificationAt = time.Unix(0, int64(millis*1e6))
			}
		}

		if sqsMsg.Body == nil {
			addDudFn(sqsMsg)
			s.log.Errorln("Received empty SQS message")
			continue
		}

		objects, err := s.parseObjectPaths(sqsMsg.Body)
		if err != nil {
			addDudFn(sqsMsg)
			s.log.Errorf("SQS extract key error: %v\n", err)
			continue
		}
		if len(objects) == 0 {
			addDudFn(sqsMsg)
			s.log.Debugln("Extracted zero target keys from SQS message")
			continue
		}

		pendingAcks := int32(len(objects))
		var nackOnce sync.Once
		for _, object := range objects {
			ackOnce := sync.Once{}
			pendingObjects = append(pendingObjects, newS3ObjectTarget(
				object.key, object.bucket, notificationAt,
				deleteS3ObjectAckFn(
					s.s3, object.bucket, object.key, s.conf.DeleteObjects,
					func(ctx context.Context, err error) (aerr error) {
						if err != nil {
							nackOnce.Do(func() {
								// Prevent future acks from triggering a delete.
								atomic.StoreInt32(&pendingAcks, -1)

								s.log.Debugf("Pushing SQS notification back into the queue due to error: %v\n", err)

								// It's possible that this is called for one message
								// at the _exact_ same time as another is acked, but
								// if the acked message triggers a full ack of the
								// origin message then even though it shouldn't be
								// possible, it's also harmless.
								aerr = s.nackSQSMessage(ctx, sqsMsg)
							})
						} else {
							ackOnce.Do(func() {
								if atomic.AddInt32(&pendingAcks, -1) == 0 {
									aerr = s.ackSQSMessage(ctx, sqsMsg)
								}
							})
						}
						return
					},
				),
			))
		}
	}

	// Discard any SQS messages not associated with a target file.
	for len(dudMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(s.conf.SQS.URL),
			Entries:  dudMessageHandles,
		}

		// trim input entries to max size
		if len(dudMessageHandles) > 10 {
			input.Entries, dudMessageHandles = dudMessageHandles[:10], dudMessageHandles[10:]
		} else {
			dudMessageHandles = nil
		}
		_, _ = s.sqs.ChangeMessageVisibilityBatch(&input)
	}

	return pendingObjects, nil
}

func (s *sqsTargetReader) nackSQSMessage(ctx context.Context, msg *sqs.Message) error {
	_, err := s.sqs.ChangeMessageVisibilityWithContext(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.conf.SQS.URL),
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(0),
	})
	return err
}

func (s *sqsTargetReader) ackSQSMessage(ctx context.Context, msg *sqs.Message) error {
	_, err := s.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.conf.SQS.URL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type awsS3Reader struct {
	conf input.AWSS3Config

	objectScannerCtor codec.ReaderConstructor
	keyReader         s3ObjectTargetReader

	session *session.Session
	s3      *s3.S3
	sqs     *sqs.SQS

	gracePeriod time.Duration

	objectMut sync.Mutex
	object    *s3PendingObject

	log log.Modular
}

type s3PendingObject struct {
	target    *s3ObjectTarget
	obj       *s3.GetObjectOutput
	extracted int
	scanner   codec.Reader
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func newAmazonS3Reader(conf input.AWSS3Config, nm bundle.NewManagement) (*awsS3Reader, error) {
	if conf.Bucket == "" && conf.SQS.URL == "" {
		return nil, errors.New("either a bucket or an sqs.url must be specified")
	}
	if conf.Prefix != "" && conf.SQS.URL != "" {
		return nil, errors.New("cannot specify both a prefix and sqs.url")
	}
	s := &awsS3Reader{
		conf: conf,
		log:  nm.Logger(),
	}

	readerConfig := codec.NewReaderConfig()
	readerConfig.MaxScanTokenSize = conf.MaxBuffer

	var err error
	if s.objectScannerCtor, err = codec.GetReader(conf.Codec, readerConfig); err != nil {
		return nil, err
	}
	if len(conf.SQS.DelayPeriod) > 0 {
		if s.gracePeriod, err = time.ParseDuration(conf.SQS.DelayPeriod); err != nil {
			return nil, fmt.Errorf("failed to parse grace period: %w", err)
		}
	}
	return s, nil
}

func (a *awsS3Reader) getTargetReader(ctx context.Context) (s3ObjectTargetReader, error) {
	if a.sqs != nil {
		return newSQSTargetReader(a.conf, a.log, a.s3, a.sqs), nil
	}
	return newStaticTargetReader(ctx, a.conf, a.log, a.s3)
}

// Connect attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (a *awsS3Reader) Connect(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.Config, func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(a.conf.ForcePathStyleURLs)
	})
	if err != nil {
		return err
	}

	a.session = sess
	a.s3 = s3.New(sess)
	if a.conf.SQS.URL != "" {
		sqsSess := sess.Copy()
		if len(a.conf.SQS.Endpoint) > 0 {
			sqsSess.Config.Endpoint = &a.conf.SQS.Endpoint
		}
		a.sqs = sqs.New(sqsSess)
	}

	if a.keyReader, err = a.getTargetReader(ctx); err != nil {
		a.session = nil
		a.s3 = nil
		a.sqs = nil
		return err
	}

	if a.conf.SQS.URL == "" {
		a.log.Infof("Downloading S3 objects from bucket: %s\n", a.conf.Bucket)
	} else {
		a.log.Infof("Downloading S3 objects found in messages from SQS: %s\n", a.conf.SQS.URL)
	}
	return nil
}

func s3MsgFromParts(p *s3PendingObject, parts []*message.Part) message.Batch {
	msg := message.Batch(parts)
	_ = msg.Iter(func(_ int, part *message.Part) error {
		part.MetaSetMut("s3_key", p.target.key)
		part.MetaSetMut("s3_bucket", p.target.bucket)
		if p.obj.LastModified != nil {
			part.MetaSetMut("s3_last_modified", p.obj.LastModified.Format(time.RFC3339))
			part.MetaSetMut("s3_last_modified_unix", p.obj.LastModified.Unix())
		}
		if p.obj.ContentType != nil {
			part.MetaSetMut("s3_content_type", *p.obj.ContentType)
		}
		if p.obj.ContentEncoding != nil {
			part.MetaSetMut("s3_content_encoding", *p.obj.ContentEncoding)
		}
		for k, v := range p.obj.Metadata {
			if v != nil {
				part.MetaSetMut(k, *v)
			}
		}
		return nil
	})
	return msg
}

func (a *awsS3Reader) getObjectTarget(ctx context.Context) (*s3PendingObject, error) {
	if a.object != nil {
		return a.object, nil
	}

	target, err := a.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	if a.gracePeriod > 0 && !target.notificationAt.IsZero() {
		waitFor := a.gracePeriod - time.Since(target.notificationAt)
		if waitFor > 0 && waitFor < a.gracePeriod {
			select {
			case <-time.After(waitFor):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	obj, err := a.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(target.bucket),
		Key:    aws.String(target.key),
	})
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	object := &s3PendingObject{
		target: target,
		obj:    obj,
	}
	if object.scanner, err = a.objectScannerCtor(target.key, obj.Body, target.ackFn); err != nil {
		// Warning: NEVER return io.EOF from a scanner constructor, as this will
		// falsely indicate that we've reached the end of our list of object
		// targets when running an SQS feed.
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("encountered an empty file for key '%v'", target.key)
		}
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

// ReadBatch attempts to read a new message from the target S3 bucket.
func (a *awsS3Reader) ReadBatch(ctx context.Context) (msg message.Batch, ackFn input.AsyncAckFn, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()
	if a.session == nil {
		return nil, nil, component.ErrNotConnected
	}

	defer func() {
		if errors.Is(err, io.EOF) {
			err = component.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = component.ErrTimeout
		}
	}()

	var object *s3PendingObject
	if object, err = a.getObjectTarget(ctx); err != nil {
		return
	}

	var parts []*message.Part
	var scnAckFn codec.ReaderAckFn

	for {
		if parts, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			object.extracted++
			break
		}
		a.object = nil
		if !errors.Is(err, io.EOF) {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			a.log.Warnf("Failed to close bucket object scanner cleanly: %v\n", err)
		}
		if object.extracted == 0 {
			a.log.Debugf("Extracted zero messages from key %v\n", object.target.key)
		}
		if object, err = a.getObjectTarget(ctx); err != nil {
			return
		}
	}

	return s3MsgFromParts(object, parts), func(rctx context.Context, res error) error {
		return scnAckFn(rctx, res)
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *awsS3Reader) Close(ctx context.Context) (err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	if a.object != nil {
		err = a.object.scanner.Close(ctx)
		a.object = nil
	}
	return
}
