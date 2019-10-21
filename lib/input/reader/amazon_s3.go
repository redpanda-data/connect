// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

// S3DownloadManagerConfig is a config struct containing fields for an S3
// download manager.
type S3DownloadManagerConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

// AmazonS3Config contains configuration values for the AmazonS3 input type.
type AmazonS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string                  `json:"bucket" yaml:"bucket"`
	Prefix             string                  `json:"prefix" yaml:"prefix"`
	Retries            int                     `json:"retries" yaml:"retries"`
	ForcePathStyleURLs bool                    `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	DownloadManager    S3DownloadManagerConfig `json:"download_manager" yaml:"download_manager"`
	DeleteObjects      bool                    `json:"delete_objects" yaml:"delete_objects"`
	SQSURL             string                  `json:"sqs_url" yaml:"sqs_url"`
	SQSEndpoint        string                  `json:"sqs_endpoint" yaml:"sqs_endpoint"`
	SQSBodyPath        string                  `json:"sqs_body_path" yaml:"sqs_body_path"`
	SQSBucketPath      string                  `json:"sqs_bucket_path" yaml:"sqs_bucket_path"`
	SQSEnvelopePath    string                  `json:"sqs_envelope_path" yaml:"sqs_envelope_path"`
	SQSMaxMessages     int64                   `json:"sqs_max_messages" yaml:"sqs_max_messages"`
	MaxBatchCount      int                     `json:"max_batch_count" yaml:"max_batch_count"`
	Timeout            string                  `json:"timeout" yaml:"timeout"`
}

// NewAmazonS3Config creates a new AmazonS3Config with default values.
func NewAmazonS3Config() AmazonS3Config {
	return AmazonS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		Prefix:             "",
		Retries:            3,
		ForcePathStyleURLs: false,
		DownloadManager: S3DownloadManagerConfig{
			Enabled: true,
		},
		DeleteObjects:   false,
		SQSURL:          "",
		SQSEndpoint:     "",
		SQSBodyPath:     "Records.*.s3.object.key",
		SQSBucketPath:   "",
		SQSEnvelopePath: "",
		SQSMaxMessages:  10,
		MaxBatchCount:   1,
		Timeout:         "5s",
	}
}

//------------------------------------------------------------------------------

type objKey struct {
	s3Key     string
	s3Bucket  string
	attempts  int
	sqsHandle *sqs.DeleteMessageBatchRequestEntry
}

// AmazonS3 is a benthos reader.Type implementation that reads messages from an
// Amazon S3 bucket.
type AmazonS3 struct {
	conf AmazonS3Config

	sqsBodyPath   string
	sqsEnvPath    string
	sqsBucketPath string

	readKeys      []objKey
	targetKeys    []objKey
	targetKeysMut sync.Mutex

	readMethod func() (types.Part, objKey, error)

	session    *session.Session
	s3         *s3.S3
	downloader *s3manager.Downloader
	sqs        *sqs.SQS
	timeout    time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3 creates a new Amazon S3 bucket reader.Type.
func NewAmazonS3(
	conf AmazonS3Config,
	log log.Modular,
	stats metrics.Type,
) (*AmazonS3, error) {
	if len(conf.SQSURL) > 0 && conf.SQSBodyPath == "Records.s3.object.key" {
		log.Warnf("It looks like a deprecated SQS Body path is configured: 'Records.s3.object.key', you might not receive S3 items unless you update to the new syntax 'Records.*.s3.object.key'")
	}

	if len(conf.Bucket) == 0 {
		return nil, errors.New("a bucket must be specified (even with an SQS bucket path configured)")
	}

	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}
	if conf.MaxBatchCount < 1 {
		return nil, fmt.Errorf("max_batch_count '%v' must be > 0", conf.MaxBatchCount)
	}
	s := &AmazonS3{
		conf:          conf,
		sqsBodyPath:   conf.SQSBodyPath,
		sqsEnvPath:    conf.SQSEnvelopePath,
		sqsBucketPath: conf.SQSBucketPath,
		log:           log,
		stats:         stats,
		timeout:       timeout,
	}
	if conf.DownloadManager.Enabled {
		s.readMethod = s.readFromMgr
	} else {
		s.readMethod = s.read
	}
	return s, nil
}

// Connect attempts to establish a connection to the target S3 bucket and any
// relevant queues used to traverse the objects (SQS, etc).
func (a *AmazonS3) Connect() error {
	return a.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (a *AmazonS3) ConnectWithContext(ctx context.Context) error {
	a.targetKeysMut.Lock()
	defer a.targetKeysMut.Unlock()

	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession(func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(a.conf.ForcePathStyleURLs)
	})
	if err != nil {
		return err
	}

	sThree := s3.New(sess)
	dler := s3manager.NewDownloader(sess)

	if len(a.conf.SQSURL) == 0 {
		listInput := &s3.ListObjectsInput{
			Bucket: aws.String(a.conf.Bucket),
		}
		if len(a.conf.Prefix) > 0 {
			listInput.Prefix = aws.String(a.conf.Prefix)
		}
		err := sThree.ListObjectsPagesWithContext(ctx, listInput,
			func(page *s3.ListObjectsOutput, isLastPage bool) bool {
				for _, obj := range page.Contents {
					a.targetKeys = append(a.targetKeys, objKey{
						s3Key:    *obj.Key,
						attempts: a.conf.Retries,
					})
				}
				return true
			},
		)
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
	} else {
		sqsSess := sess.Copy()
		if len(a.conf.SQSEndpoint) > 0 {
			sqsSess.Config.Endpoint = &a.conf.SQSEndpoint
		}
		a.sqs = sqs.New(sqsSess)
	}

	a.log.Infof("Receiving Amazon S3 objects from bucket: %s\n", a.conf.Bucket)

	a.session = sess
	a.downloader = dler
	a.s3 = sThree
	return nil
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

type objTarget struct {
	key    string
	bucket string
}

func (a *AmazonS3) parseItemPaths(sqsMsg *string) ([]objTarget, error) {
	gObj, err := gabs.ParseJSON([]byte(*sqsMsg))
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQS message: %v", err)
	}

	if len(a.sqsEnvPath) > 0 {
		switch t := gObj.Path(a.sqsEnvPath).Data().(type) {
		case string:
			if gObj, err = gabs.ParseJSON([]byte(t)); err != nil {
				return nil, fmt.Errorf("failed to parse SQS message envelope: %v", err)
			}
		case []interface{}:
			docs := []interface{}{}
			strs := digStrsFromSlices(t)
			for _, v := range strs {
				var gObj2 interface{}
				if err2 := json.Unmarshal([]byte(v), &gObj2); err2 == nil {
					docs = append(docs, gObj2)
				}
			}
			if len(docs) == 0 {
				return nil, errors.New("couldn't locate S3 items from SQS message")
			}
			gObj = gabs.Wrap(docs)
		default:
			return nil, fmt.Errorf("unexpected envelope value: %v", t)
		}
	}

	var buckets []string
	switch t := gObj.Path(a.sqsBucketPath).Data().(type) {
	case string:
		buckets = []string{t}
	case []interface{}:
		buckets = digStrsFromSlices(t)
	}

	items := []objTarget{}

	switch t := gObj.Path(a.sqsBodyPath).Data().(type) {
	case string:
		if strings.HasPrefix(t, a.conf.Prefix) {
			bucket := ""
			if len(buckets) > 0 {
				bucket = buckets[0]
			}
			items = append(items, objTarget{
				key:    t,
				bucket: bucket,
			})
		}
	case []interface{}:
		newTargets := []string{}
		strs := digStrsFromSlices(t)
		for _, p := range strs {
			if strings.HasPrefix(p, a.conf.Prefix) {
				newTargets = append(newTargets, p)
			}
		}
		if len(newTargets) > 0 {
			for i, target := range newTargets {
				bucket := ""
				if len(buckets) > i {
					bucket = buckets[i]
				}
				decodedTarget, err := url.QueryUnescape(target)
				if err != nil {
					return nil, fmt.Errorf("failed to decode S3 path: %v", err)
				}
				items = append(items, objTarget{
					key:    decodedTarget,
					bucket: bucket,
				})
			}
		} else {
			return nil, errors.New("no items found in SQS message at specified path")
		}
	default:
		return nil, errors.New("no items found in SQS message at specified path")
	}
	return items, nil
}

func (a *AmazonS3) rejectObjects(keys []objKey) {
	ctx, done := context.WithTimeout(context.Background(), a.timeout)
	defer done()

	var failedMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	for _, key := range keys {
		failedMessageHandles = append(failedMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                key.sqsHandle.Id,
			ReceiptHandle:     key.sqsHandle.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}
	for len(failedMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.SQSURL),
			Entries:  failedMessageHandles,
		}

		// trim input entries to max size
		if len(failedMessageHandles) > 10 {
			input.Entries, failedMessageHandles = failedMessageHandles[:10], failedMessageHandles[10:]
		} else {
			failedMessageHandles = nil
		}
		if _, err := a.sqs.ChangeMessageVisibilityBatchWithContext(ctx, &input); err != nil {
			a.log.Errorf("Failed to reject SQS message: %v\n", err)
		}
	}
}

func (a *AmazonS3) deleteObjects(keys []objKey) {
	ctx, done := context.WithTimeout(context.Background(), a.timeout)
	defer done()

	deleteHandles := []*sqs.DeleteMessageBatchRequestEntry{}
	for _, key := range keys {
		if a.conf.DeleteObjects {
			bucket := a.conf.Bucket
			if len(key.s3Bucket) > 0 {
				bucket = key.s3Bucket
			}
			if _, serr := a.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key.s3Key),
			}); serr != nil {
				a.log.Errorf("Failed to delete consumed object: %v\n", serr)
			}
		}
		if key.sqsHandle != nil {
			deleteHandles = append(deleteHandles, key.sqsHandle)
		}
	}
	for len(deleteHandles) > 0 {
		input := sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(a.conf.SQSURL),
			Entries:  deleteHandles,
		}

		// trim input entries to max size
		if len(deleteHandles) > 10 {
			input.Entries, deleteHandles = deleteHandles[:10], deleteHandles[10:]
		} else {
			deleteHandles = nil
		}

		if res, serr := a.sqs.DeleteMessageBatchWithContext(ctx, &input); serr != nil {
			a.log.Errorf("Failed to delete consumed SQS messages: %v\n", serr)
		} else {
			for _, fail := range res.Failed {
				a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", *fail.Id, *fail.Code)
			}
		}
	}
}

func (a *AmazonS3) readSQSEvents() error {
	var dudMessageHandles []*sqs.ChangeMessageVisibilityBatchRequestEntry
	addDudFn := func(m *sqs.Message) {
		dudMessageHandles = append(dudMessageHandles, &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                m.MessageId,
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: aws.Int64(0),
		})
	}

	output, err := a.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.conf.SQSURL),
		MaxNumberOfMessages: aws.Int64(a.conf.SQSMaxMessages),
		WaitTimeSeconds:     aws.Int64(int64(a.timeout.Seconds())),
	})
	if err != nil {
		return err
	}

messageLoop:
	for _, sqsMsg := range output.Messages {
		msgHandle := &sqs.DeleteMessageBatchRequestEntry{
			Id:            sqsMsg.MessageId,
			ReceiptHandle: sqsMsg.ReceiptHandle,
		}

		if sqsMsg.Body == nil {
			addDudFn(sqsMsg)
			a.log.Errorln("Received empty SQS message")
			continue messageLoop
		}

		items, err := a.parseItemPaths(sqsMsg.Body)
		if err != nil {
			addDudFn(sqsMsg)
			a.log.Errorf("SQS error: %v\n", err)
			continue messageLoop
		}

		for _, item := range items {
			a.targetKeys = append(a.targetKeys, objKey{
				s3Key:    item.key,
				s3Bucket: item.bucket,
				attempts: a.conf.Retries,
			})
		}
		a.targetKeys[len(a.targetKeys)-1].sqsHandle = msgHandle
	}

	// Discard any SQS messages not associated with a target file.
	for len(dudMessageHandles) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.SQSURL),
			Entries:  dudMessageHandles,
		}

		// trim input entries to max size
		if len(dudMessageHandles) > 10 {
			input.Entries, dudMessageHandles = dudMessageHandles[:10], dudMessageHandles[10:]
		} else {
			dudMessageHandles = nil
		}
		a.sqs.ChangeMessageVisibilityBatch(&input)
	}

	if len(a.targetKeys) == 0 {
		return types.ErrTimeout
	}
	return nil
}

func (a *AmazonS3) pushReadKey(key objKey) {
	a.readKeys = append(a.readKeys, key)
}

func (a *AmazonS3) popTargetKey() {
	if len(a.targetKeys) == 0 {
		return
	}
	if len(a.targetKeys) > 1 {
		a.targetKeys = a.targetKeys[1:]
	} else {
		a.targetKeys = nil
	}
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (a *AmazonS3) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	a.targetKeysMut.Lock()
	defer a.targetKeysMut.Unlock()

	if a.session == nil {
		return nil, nil, types.ErrNotConnected
	}

	if len(a.targetKeys) == 0 {
		if a.sqs != nil {
			if err := a.readSQSEvents(); err != nil {
				return nil, nil, err
			}
		} else {
			// If we aren't using SQS but exhausted our targets we are done.
			return nil, nil, types.ErrTypeClosed
		}
	}
	if len(a.targetKeys) == 0 {
		return nil, nil, types.ErrTimeout
	}

	msg := message.New(nil)

	part, obj, err := a.readMethod()
	if err != nil {
		return nil, nil, err
	}

	msg.Append(part)
	return msg, func(rctx context.Context, res types.Response) error {
		if res.Error() == nil {
			a.deleteObjects([]objKey{obj})
		} else {
			if len(a.conf.SQSURL) == 0 {
				a.targetKeysMut.Lock()
				a.targetKeys = append(a.readKeys, obj)
				a.targetKeysMut.Unlock()
			} else {
				a.rejectObjects([]objKey{obj})
			}
		}
		return nil
	}, nil
}

// Read attempts to read a new message from the target S3 bucket.
func (a *AmazonS3) Read() (types.Message, error) {
	a.targetKeysMut.Lock()
	defer a.targetKeysMut.Unlock()

	if a.session == nil {
		return nil, types.ErrNotConnected
	}

	timeoutAt := time.Now().Add(a.timeout)

	if len(a.targetKeys) == 0 {
		if a.sqs != nil {
			if err := a.readSQSEvents(); err != nil {
				return nil, err
			}
		} else {
			// If we aren't using SQS but exhausted our targets we are done.
			return nil, types.ErrTypeClosed
		}
	}
	if len(a.targetKeys) == 0 {
		return nil, types.ErrTimeout
	}

	msg := message.New(nil)

	for len(a.targetKeys) > 0 && msg.Len() < a.conf.MaxBatchCount && time.Until(timeoutAt) > 0 {
		part, objKey, err := a.readMethod()
		if err != nil {
			if err == types.ErrTimeout {
				break
			}
			a.log.Errorf("Error: %v\n", err)
			if msg.Len() == 0 {
				return nil, err
			}
		} else {
			msg.Append(part)
			a.pushReadKey(objKey)
		}
	}
	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}

	return msg, nil
}

func addS3Metadata(p types.Part, obj *s3.GetObjectOutput) {
	meta := p.Metadata()
	if obj.LastModified != nil {
		meta.Set("s3_last_modified", obj.LastModified.Format(time.RFC3339))
		meta.Set("s3_last_modified_unix", strconv.FormatInt(obj.LastModified.Unix(), 10))
	}
	if obj.ContentType != nil {
		meta.Set("s3_content_type", *obj.ContentType)
	}
	if obj.ContentEncoding != nil {
		meta.Set("s3_content_encoding", *obj.ContentEncoding)
	}
}

// read attempts to read a new message from the target S3 bucket.
func (a *AmazonS3) read() (types.Part, objKey, error) {
	target := a.targetKeys[0]

	bucket := a.conf.Bucket
	if len(target.s3Bucket) > 0 {
		bucket = target.s3Bucket
	}
	obj, err := a.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(target.s3Key),
	})
	if err != nil {
		target.attempts--
		if target.attempts == 0 {
			// Remove the target file from our list.
			a.popTargetKey()
			a.log.Errorf("Failed to download file '%s' from bucket '%s' after '%v' attempts: %v\n", target.s3Key, bucket, a.conf.Retries, err)
		} else {
			a.targetKeys[0] = target
			return nil, objKey{}, fmt.Errorf("failed to download file '%s' from bucket '%s': %v", target.s3Key, bucket, err)
		}
		return nil, objKey{}, types.ErrTimeout
	}

	bytes, err := ioutil.ReadAll(obj.Body)
	obj.Body.Close()
	if err != nil {
		a.popTargetKey()
		return nil, objKey{}, fmt.Errorf("failed to download file '%s' from bucket '%s': %v", target.s3Key, bucket, err)
	}

	part := message.NewPart(bytes)
	meta := part.Metadata()
	for k, v := range obj.Metadata {
		meta.Set(k, *v)
	}
	meta.Set("s3_key", target.s3Key)
	meta.Set("s3_bucket", bucket)
	addS3Metadata(part, obj)

	a.popTargetKey()
	return part, target, nil
}

// readFromMgr attempts to read a new message from the target S3 bucket using a
// download manager.
func (a *AmazonS3) readFromMgr() (types.Part, objKey, error) {
	target := a.targetKeys[0]

	buff := &aws.WriteAtBuffer{}

	bucket := a.conf.Bucket
	if len(target.s3Bucket) > 0 {
		bucket = target.s3Bucket
	}

	// Write the contents of S3 Object to the file
	if _, err := a.downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(target.s3Key),
	}); err != nil {
		target.attempts--
		if target.attempts == 0 {
			// Remove the target file from our list.
			a.popTargetKey()
			a.log.Errorf("Failed to download file '%s' from bucket '%s' after '%v' attempts: %v\n", target.s3Key, bucket, a.conf.Retries, err)
		} else {
			a.targetKeys[0] = target
			return nil, objKey{}, fmt.Errorf("failed to download file '%s' from bucket '%s': %v", target.s3Key, bucket, err)
		}
		return nil, objKey{}, types.ErrTimeout
	}

	part := message.NewPart(buff.Bytes())
	part.Metadata().
		Set("s3_key", target.s3Key).
		Set("s3_bucket", bucket)

	a.popTargetKey()
	return part, target, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (a *AmazonS3) Acknowledge(err error) error {
	if err == nil {
		a.deleteObjects(a.readKeys)
	} else {
		if a.sqs == nil {
			a.targetKeysMut.Lock()
			a.targetKeys = append(a.readKeys, a.targetKeys...)
			a.targetKeysMut.Unlock()
		} else {
			a.rejectObjects(a.readKeys)
		}
	}
	a.readKeys = nil
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonS3) CloseAsync() {
	go func() {
		a.targetKeysMut.Lock()
		a.rejectObjects(a.targetKeys)
		a.targetKeys = nil
		a.targetKeysMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
