package input

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

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func init() {
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			var r reader.Async
			var err error
			if r, err = newAmazonS3(conf.AWSS3, log, stats); err != nil {
				return nil, err
			}
			// If we're not pulling events directly from an SQS queue then
			// there's no concept of propagating nacks upstream, therefore wrap
			// our reader within a preserver in order to retry indefinitely.
			if conf.AWSS3.SQS.URL == "" {
				r = reader.NewAsyncPreserver(r)
			}
			return NewAsyncReader(TypeAWSS3, false, r, log, stats)
		}),
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

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata). Note that user defined metadata is case insensitive within AWS, and it is likely that the keys will be received in a capitalized form, if you wish to make them consistent you can map all metadata keys to lower or uppercase using a Bloblang mapping such as ` + "`meta = meta().map_each_key(key -> key.lowercase())`" + `.`,

		FieldSpecs: append(
			append(docs.FieldSpecs{
				docs.FieldCommon("bucket", "The bucket to consume from. If the field `sqs.url` is specified this field is optional."),
				docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed when walking a bucket."),
			}, sess.FieldSpecs()...),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs for downloading keys, which is often required when connecting to custom endpoints."),
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed."),
			codec.ReaderDocs,
			docs.FieldCommon("sqs", "Consume SQS messages in order to trigger key downloads.").WithChildren(
				docs.FieldCommon("url", "An optional SQS URL to connect to. When specified this queue will control which objects are downloaded."),
				docs.FieldAdvanced("endpoint", "A custom endpoint to use when connecting to SQS."),
				docs.FieldCommon("key_path", "A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages."),
				docs.FieldCommon("bucket_path", "A [dot path](/docs/configuration/field_paths) whereby the bucket name can be found in SQS messages."),
				docs.FieldCommon("envelope_path", "A [dot path](/docs/configuration/field_paths) of a field to extract an enveloped JSON payload for further extracting the key and bucket from SQS messages. This is specifically useful when subscribing an SQS queue to an SNS topic that receives bucket events.", "Message"),
				docs.FieldAdvanced(
					"delay_period",
					"An optional period of time to wait from when a notification was originally sent to when the target key download is attempted.",
					"10s", "5m",
				),
				docs.FieldAdvanced("max_messages", "The maximum number of SQS messages to consume from each request."),
			),
		),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// AWSS3SQSConfig contains configuration for hooking up the S3 input with an SQS queue.
type AWSS3SQSConfig struct {
	URL          string `json:"url" yaml:"url"`
	Endpoint     string `json:"endpoint" yaml:"endpoint"`
	EnvelopePath string `json:"envelope_path" yaml:"envelope_path"`
	KeyPath      string `json:"key_path" yaml:"key_path"`
	BucketPath   string `json:"bucket_path" yaml:"bucket_path"`
	DelayPeriod  string `json:"delay_period" yaml:"delay_period"`
	MaxMessages  int64  `json:"max_messages" yaml:"max_messages"`
}

// NewAWSS3SQSConfig creates a new AWSS3SQSConfig with default values.
func NewAWSS3SQSConfig() AWSS3SQSConfig {
	return AWSS3SQSConfig{
		URL:          "",
		Endpoint:     "",
		EnvelopePath: "",
		KeyPath:      "Records.*.s3.object.key",
		BucketPath:   "Records.*.s3.bucket.name",
		DelayPeriod:  "",
		MaxMessages:  10,
	}
}

// AWSS3Config contains configuration values for the aws_s3 input type.
type AWSS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string         `json:"bucket" yaml:"bucket"`
	Codec              string         `json:"codec" yaml:"codec"`
	Prefix             string         `json:"prefix" yaml:"prefix"`
	ForcePathStyleURLs bool           `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	DeleteObjects      bool           `json:"delete_objects" yaml:"delete_objects"`
	SQS                AWSS3SQSConfig `json:"sqs" yaml:"sqs"`
}

// NewAWSS3Config creates a new AWSS3Config with default values.
func NewAWSS3Config() AWSS3Config {
	return AWSS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		Prefix:             "",
		Codec:              "all-bytes",
		ForcePathStyleURLs: false,
		DeleteObjects:      false,
		SQS:                NewAWSS3SQSConfig(),
	}
}

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
	return &s3ObjectTarget{key, bucket, notificationAt, ackFn}
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
	conf       AWSS3Config
	startAfter *string
}

func newStaticTargetReader(
	ctx context.Context,
	conf AWSS3Config,
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
	conf AWSS3Config
	log  log.Modular
	sqs  *sqs.SQS
	s3   *s3.S3

	nextRequest time.Time

	pending []*s3ObjectTarget
}

func newSQSTargetReader(
	conf AWSS3Config,
	log log.Modular,
	s3 *s3.S3,
	sqs *sqs.SQS,
) *sqsTargetReader {
	return &sqsTargetReader{conf, log, sqs, s3, time.Time{}, nil}
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

func digStrsFromSlices(slice []interface{}) []string {
	var strs []string
	for _, v := range slice {
		switch t := v.(type) {
		case []interface{}:
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
	case []interface{}:
		keys = digStrsFromSlices(t)
	}
	if len(s.conf.SQS.BucketPath) > 0 {
		switch t := gObj.Path(s.conf.SQS.BucketPath).Data().(type) {
		case string:
			buckets = []string{t}
		case []interface{}:
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
		s.sqs.ChangeMessageVisibilityBatch(&input)
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
type awsS3 struct {
	conf AWSS3Config

	objectScannerCtor codec.ReaderConstructor
	keyReader         s3ObjectTargetReader

	session *session.Session
	s3      *s3.S3
	sqs     *sqs.SQS

	gracePeriod time.Duration

	objectMut sync.Mutex
	object    *s3PendingObject

	log   log.Modular
	stats metrics.Type
}

type s3PendingObject struct {
	target    *s3ObjectTarget
	obj       *s3.GetObjectOutput
	extracted int
	scanner   codec.Reader
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func newAmazonS3(
	conf AWSS3Config,
	log log.Modular,
	stats metrics.Type,
) (*awsS3, error) {
	if conf.Bucket == "" && conf.SQS.URL == "" {
		return nil, errors.New("either a bucket or an sqs.url must be specified")
	}
	if conf.Prefix != "" && conf.SQS.URL != "" {
		return nil, errors.New("cannot specify both a prefix and sqs.url")
	}
	s := &awsS3{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	var err error
	if s.objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, err
	}
	if len(conf.SQS.DelayPeriod) > 0 {
		if s.gracePeriod, err = time.ParseDuration(conf.SQS.DelayPeriod); err != nil {
			return nil, fmt.Errorf("failed to parse grace period: %w", err)
		}
	}
	return s, nil
}

func (a *awsS3) getTargetReader(ctx context.Context) (s3ObjectTargetReader, error) {
	if a.sqs != nil {
		return newSQSTargetReader(a.conf, a.log, a.s3, a.sqs), nil
	}
	return newStaticTargetReader(ctx, a.conf, a.log, a.s3)
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (a *awsS3) ConnectWithContext(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession(func(c *aws.Config) {
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

func s3MsgFromParts(p *s3PendingObject, parts []*message.Part) *message.Batch {
	msg := message.QuickBatch(nil)
	msg.Append(parts...)
	_ = msg.Iter(func(_ int, part *message.Part) error {
		part.MetaSet("s3_key", p.target.key)
		part.MetaSet("s3_bucket", p.target.bucket)
		if p.obj.LastModified != nil {
			part.MetaSet("s3_last_modified", p.obj.LastModified.Format(time.RFC3339))
			part.MetaSet("s3_last_modified_unix", strconv.FormatInt(p.obj.LastModified.Unix(), 10))
		}
		if p.obj.ContentType != nil {
			part.MetaSet("s3_content_type", *p.obj.ContentType)
		}
		if p.obj.ContentEncoding != nil {
			part.MetaSet("s3_content_encoding", *p.obj.ContentEncoding)
		}
		for k, v := range p.obj.Metadata {
			if v != nil {
				part.MetaSet(k, *v)
			}
		}
		return nil
	})
	return msg
}

func (a *awsS3) getObjectTarget(ctx context.Context) (*s3PendingObject, error) {
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
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (a *awsS3) ReadWithContext(ctx context.Context) (msg *message.Batch, ackFn reader.AsyncAckFn, err error) {
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
		if err != io.EOF {
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

	return s3MsgFromParts(object, parts), func(rctx context.Context, res types.Response) error {
		return scnAckFn(rctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *awsS3) CloseAsync() {
	go func() {
		a.objectMut.Lock()
		if a.object != nil {
			a.object.scanner.Close(context.Background())
			a.object = nil
		}
		a.objectMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *awsS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
