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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// S3 Input SQS Fields
	s3iSQSFieldURL             = "url"
	s3iSQSFieldEndpoint        = "endpoint"
	s3iSQSFieldEnvelopePath    = "envelope_path"
	s3iSQSFieldKeyPath         = "key_path"
	s3iSQSFieldBucketPath      = "bucket_path"
	s3iSQSFieldDelayPeriod     = "delay_period"
	s3iSQSFieldMaxMessages     = "max_messages"
	s3iSQSFieldWaitTimeSeconds = "wait_time_seconds"

	// S3 Input Fields
	s3iFieldBucket             = "bucket"
	s3iFieldPrefix             = "prefix"
	s3iFieldForcePathStyleURLs = "force_path_style_urls"
	s3iFieldDeleteObjects      = "delete_objects"
	s3iFieldSQS                = "sqs"
)

type s3iSQSConfig struct {
	URL             string
	Endpoint        string
	EnvelopePath    string
	KeyPath         string
	BucketPath      string
	DelayPeriod     string
	MaxMessages     int64
	WaitTimeSeconds int64
}

func s3iSQSConfigFromParsed(pConf *service.ParsedConfig) (conf s3iSQSConfig, err error) {
	if conf.URL, err = pConf.FieldString(s3iSQSFieldURL); err != nil {
		return
	}
	if conf.Endpoint, err = pConf.FieldString(s3iSQSFieldEndpoint); err != nil {
		return
	}
	if conf.EnvelopePath, err = pConf.FieldString(s3iSQSFieldEnvelopePath); err != nil {
		return
	}
	if conf.KeyPath, err = pConf.FieldString(s3iSQSFieldKeyPath); err != nil {
		return
	}
	if conf.BucketPath, err = pConf.FieldString(s3iSQSFieldBucketPath); err != nil {
		return
	}
	if conf.DelayPeriod, err = pConf.FieldString(s3iSQSFieldDelayPeriod); err != nil {
		return
	}
	if conf.MaxMessages, err = int64Field(pConf, s3iSQSFieldMaxMessages); err != nil {
		return
	}
	if conf.WaitTimeSeconds, err = int64Field(pConf, s3iSQSFieldWaitTimeSeconds); err != nil {
		return
	}
	return
}

type s3iConfig struct {
	Bucket             string
	Prefix             string
	ForcePathStyleURLs bool
	DeleteObjects      bool
	SQS                s3iSQSConfig
	CodecCtor          interop.FallbackReaderCodec
}

func s3iConfigFromParsed(pConf *service.ParsedConfig) (conf s3iConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(s3iFieldBucket); err != nil {
		return
	}
	if conf.Prefix, err = pConf.FieldString(s3iFieldPrefix); err != nil {
		return
	}
	if conf.CodecCtor, err = interop.OldReaderCodecFromParsed(pConf); err != nil {
		return
	}
	if conf.ForcePathStyleURLs, err = pConf.FieldBool(s3iFieldForcePathStyleURLs); err != nil {
		return
	}
	if conf.DeleteObjects, err = pConf.FieldBool(s3iFieldDeleteObjects); err != nil {
		return
	}
	if pConf.Contains(s3iFieldSQS) {
		if conf.SQS, err = s3iSQSConfigFromParsed(pConf.Namespace(s3iFieldSQS)); err != nil {
			return
		}
	}
	return
}

func s3InputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Summary(`Downloads objects within an Amazon S3 bucket, optionally filtered by a prefix, either by walking the items in the bucket or by streaming upload notifications in realtime.`).
		Description(`
## Streaming Objects on Upload with SQS

A common pattern for consuming S3 objects is to emit upload notification events from the bucket either directly to an SQS queue, or to an SNS topic that is consumed by an SQS queue, and then have your consumer listen for events which prompt it to download the newly uploaded objects. More information about this pattern and how to set it up can be found at: https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html.

Benthos is able to follow this pattern when you configure an `+"`sqs.url`"+`, where it consumes events from SQS and only downloads object keys received within those events. In order for this to work Benthos needs to know where within the event the key and bucket names can be found, specified as [dot paths](/docs/configuration/field_paths) with the fields `+"`sqs.key_path` and `sqs.bucket_path`"+`. The default values for these fields should already be correct when following the guide above.

If your notification events are being routed to SQS via an SNS topic then the events will be enveloped by SNS, in which case you also need to specify the field `+"`sqs.envelope_path`"+`, which in the case of SNS to SQS will usually be `+"`Message`"+`.

When using SQS please make sure you have sensible values for `+"`sqs.max_messages`"+` and also the visibility timeout of the queue itself. When Benthos consumes an S3 object the SQS message that triggered it is not deleted until the S3 object has been sent onwards. This ensures at-least-once crash resiliency, but also means that if the S3 object takes longer to process than the visibility timeout of your queue then the same objects might be processed multiple times.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a `+"[`codec`](#codec)"+` can be specified that determines how to break the input into smaller individual messages.

## Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).

## Metadata

This input adds the following metadata fields to each message:

`+"```"+`
- s3_key
- s3_bucket
- s3_last_modified_unix
- s3_last_modified (RFC3339)
- s3_content_type
- s3_content_encoding
- s3_version_id
- All user defined metadata
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries). Note that user defined metadata is case insensitive within AWS, and it is likely that the keys will be received in a capitalized form, if you wish to make them consistent you can map all metadata keys to lower or uppercase using a Bloblang mapping such as `+"`meta = meta().map_each_key(key -> key.lowercase())`"+`.`).
		Fields(
			service.NewStringField(s3iFieldBucket).
				Description("The bucket to consume from. If the field `sqs.url` is specified this field is optional.").
				Default(""),
			service.NewStringField(s3iFieldPrefix).
				Description("An optional path prefix, if set only objects with the prefix are consumed when walking a bucket.").
				Default(""),
		).
		Fields(config.SessionFields()...).
		Fields(
			service.NewBoolField(s3iFieldForcePathStyleURLs).
				Description("Forces the client API to use path style URLs for downloading keys, which is often required when connecting to custom endpoints.").
				Default(false).
				Advanced(),
			service.NewBoolField(s3iFieldDeleteObjects).
				Description("Whether to delete downloaded objects from the bucket once they are processed.").
				Default(false).
				Advanced(),
		).
		Fields(interop.OldReaderCodecFields("to_the_end")...).
		Fields(
			service.NewObjectField(s3iFieldSQS,
				service.NewStringField(s3iSQSFieldURL).
					Description("An optional SQS URL to connect to. When specified this queue will control which objects are downloaded.").
					Default(""),
				service.NewStringField(s3iSQSFieldEndpoint).
					Description("A custom endpoint to use when connecting to SQS.").
					Default("").
					Advanced(),
				service.NewStringField(s3iSQSFieldKeyPath).
					Description("A [dot path](/docs/configuration/field_paths) whereby object keys are found in SQS messages.").
					Default("Records.*.s3.object.key"),
				service.NewStringField(s3iSQSFieldBucketPath).
					Description("A [dot path](/docs/configuration/field_paths) whereby the bucket name can be found in SQS messages.").
					Default("Records.*.s3.bucket.name"),
				service.NewStringField(s3iSQSFieldEnvelopePath).
					Description("A [dot path](/docs/configuration/field_paths) of a field to extract an enveloped JSON payload for further extracting the key and bucket from SQS messages. This is specifically useful when subscribing an SQS queue to an SNS topic that receives bucket events.").
					Default("").
					Example("Message"),
				service.NewStringField(s3iSQSFieldDelayPeriod).
					Description("An optional period of time to wait from when a notification was originally sent to when the target key download is attempted.").
					Example("10s").
					Example("5m").
					Default("").
					Advanced(),
				service.NewIntField(s3iSQSFieldMaxMessages).
					Description("The maximum number of SQS messages to consume from each request.").
					Default(10).
					Advanced(),
				service.NewIntField(s3iSQSFieldWaitTimeSeconds).
					Description("Whether to set the wait time. Enabling this activates long-polling. Valid values: 0 to 20.").
					Default(0).
					Advanced(),
			).
				Description("Consume SQS messages in order to trigger key downloads.").
				Optional(),
		)
}

func init() {
	err := service.RegisterBatchInput("aws_s3", s3InputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			conf, err := s3iConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			sess, err := GetSession(context.Background(), pConf)
			if err != nil {
				return nil, err
			}

			var rdr service.BatchInput
			if rdr, err = newAmazonS3Reader(conf, sess, res); err != nil {
				return nil, err
			}

			// If we're not pulling events directly from an SQS queue then
			// there's no concept of propagating nacks upstream, therefore wrap
			// our reader within a preserver in order to retry indefinitely.
			if conf.SQS.URL == "" {
				rdr = service.AutoRetryNacksBatched(rdr)
			}
			return rdr, nil
		})
	if err != nil {
		panic(err)
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
	return &s3ObjectTarget{key: key, bucket: bucket, notificationAt: notificationAt, ackFn: ackFn}
}

type s3ObjectTargetReader interface {
	Pop(ctx context.Context) (*s3ObjectTarget, error)
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

func deleteS3ObjectAckFn(
	s3Client *s3.Client,
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
		_, aerr := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		return aerr
	}
}

//------------------------------------------------------------------------------

type staticTargetReader struct {
	pending    []*s3ObjectTarget
	s3         *s3.Client
	conf       s3iConfig
	startAfter *string
}

func newStaticTargetReader(
	ctx context.Context,
	conf s3iConfig,
	log *service.Logger,
	s3Client *s3.Client,
) (*staticTargetReader, error) {
	maxKeys := int32(100)
	listInput := &s3.ListObjectsV2Input{
		Bucket:  &conf.Bucket,
		MaxKeys: &maxKeys,
	}
	if len(conf.Prefix) > 0 {
		listInput.Prefix = &conf.Prefix
	}
	output, err := s3Client.ListObjectsV2(ctx, listInput)
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
	maxKeys := int32(100)
	if len(s.pending) == 0 && s.startAfter != nil {
		s.pending = nil
		listInput := &s3.ListObjectsV2Input{
			Bucket:     &s.conf.Bucket,
			MaxKeys:    &maxKeys,
			StartAfter: s.startAfter,
		}
		if len(s.conf.Prefix) > 0 {
			listInput.Prefix = &s.conf.Prefix
		}
		output, err := s.s3.ListObjectsV2(ctx, listInput)
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
	conf s3iConfig
	log  *service.Logger
	sqs  *sqs.Client
	s3   *s3.Client

	nextRequest time.Time

	pending []*s3ObjectTarget
}

func newSQSTargetReader(
	conf s3iConfig,
	log *service.Logger,
	s3 *s3.Client,
	sqs *sqs.Client,
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
	var dudMessageHandles []sqstypes.ChangeMessageVisibilityBatchRequestEntry
	addDudFn := func(m sqstypes.Message) {
		dudMessageHandles = append(dudMessageHandles, sqstypes.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: 0,
		})
	}

	output, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &s.conf.SQS.URL,
		MaxNumberOfMessages: int32(s.conf.SQS.MaxMessages),
		WaitTimeSeconds:     int32(s.conf.SQS.WaitTimeSeconds),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeName(sqstypes.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []string{
			string(sqstypes.MessageSystemAttributeNameSentTimestamp),
		},
	})
	if err != nil {
		return nil, err
	}

	var pendingObjects []*s3ObjectTarget

	for _, sqsMsg := range output.Messages {
		sqsMsg := sqsMsg

		var notificationAt time.Time
		if rcvd, ok := sqsMsg.Attributes["SentTimestamp"]; ok {
			if millis, _ := strconv.Atoi(rcvd); millis > 0 {
				notificationAt = time.Unix(0, int64(millis*1e6))
			}
		}

		if sqsMsg.Body == nil {
			addDudFn(sqsMsg)
			s.log.Error("Received empty SQS message")
			continue
		}

		objects, err := s.parseObjectPaths(sqsMsg.Body)
		if err != nil {
			addDudFn(sqsMsg)
			s.log.Errorf("SQS extract key error: %v", err)
			continue
		}
		if len(objects) == 0 {
			addDudFn(sqsMsg)
			s.log.Debug("Extracted zero target keys from SQS message")
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
		_, _ = s.sqs.ChangeMessageVisibilityBatch(ctx, &input)
	}

	return pendingObjects, nil
}

func (s *sqsTargetReader) nackSQSMessage(ctx context.Context, msg sqstypes.Message) error {
	_, err := s.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &s.conf.SQS.URL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: 0,
	})
	return err
}

func (s *sqsTargetReader) ackSQSMessage(ctx context.Context, msg sqstypes.Message) error {
	_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.conf.SQS.URL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type awsS3Reader struct {
	conf s3iConfig

	objectScannerCtor interop.FallbackReaderCodec
	keyReader         s3ObjectTargetReader

	awsConf aws.Config
	s3      *s3.Client
	sqs     *sqs.Client

	gracePeriod time.Duration

	objectMut sync.Mutex
	object    *s3PendingObject

	log *service.Logger
}

type s3PendingObject struct {
	target    *s3ObjectTarget
	obj       *s3.GetObjectOutput
	extracted int
	scanner   interop.FallbackReaderStream
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func newAmazonS3Reader(conf s3iConfig, awsConf aws.Config, nm *service.Resources) (*awsS3Reader, error) {
	if conf.Bucket == "" && conf.SQS.URL == "" {
		return nil, errors.New("either a bucket or an sqs.url must be specified")
	}
	if conf.Prefix != "" && conf.SQS.URL != "" {
		return nil, errors.New("cannot specify both a prefix and sqs.url")
	}
	s := &awsS3Reader{
		conf:              conf,
		awsConf:           awsConf,
		log:               nm.Logger(),
		objectScannerCtor: conf.CodecCtor,
	}
	if len(conf.SQS.DelayPeriod) > 0 {
		var err error
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
	if a.s3 != nil {
		return nil
	}

	a.s3 = s3.NewFromConfig(a.awsConf, func(o *s3.Options) {
		o.UsePathStyle = a.conf.ForcePathStyleURLs
	})
	if a.conf.SQS.URL != "" {
		sqsConf := a.awsConf.Copy()
		if len(a.conf.SQS.Endpoint) > 0 {
			sqsConf.BaseEndpoint = &a.conf.SQS.Endpoint
		}
		a.sqs = sqs.NewFromConfig(sqsConf)
	}

	var err error
	if a.keyReader, err = a.getTargetReader(ctx); err != nil {
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

func s3MetaToBatch(p *s3PendingObject, parts service.MessageBatch) {
	for _, part := range parts {
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
		if p.obj.VersionId != nil && *p.obj.VersionId != "null" {
			part.MetaSetMut("s3_version_id", *p.obj.VersionId)
		}
		for k, v := range p.obj.Metadata {
			part.MetaSetMut(k, v)
		}
	}
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

	obj, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
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
	if object.scanner, err = a.objectScannerCtor.Create(obj.Body, target.ackFn, scanner.SourceDetails{
		Name: target.key,
	}); err != nil {
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
func (a *awsS3Reader) ReadBatch(ctx context.Context) (msg service.MessageBatch, ackFn service.AckFunc, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()
	if a.s3 == nil {
		return nil, nil, service.ErrNotConnected
	}

	defer func() {
		if errors.Is(err, io.EOF) {
			err = service.ErrEndOfInput
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

	var resBatch service.MessageBatch
	var scnAckFn service.AckFunc

	for {
		if resBatch, scnAckFn, err = object.scanner.NextBatch(ctx); err == nil {
			object.extracted++
			break
		}
		a.object = nil
		if !errors.Is(err, io.EOF) {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			a.log.Warnf("Failed to close bucket object scanner cleanly: %v", err)
		}
		if object.extracted == 0 {
			a.log.Debugf("Extracted zero messages from key %v", object.target.key)
		}
		if object, err = a.getObjectTarget(ctx); err != nil {
			return
		}
	}

	s3MetaToBatch(object, resBatch)

	return resBatch, func(rctx context.Context, res error) error {
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
