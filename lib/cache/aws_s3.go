package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSS3] = TypeSpec{
		constructor: NewAWSS3,
		Version:     "3.36.0",
		Summary: `
Stores each item in an S3 bucket as a file, where an item ID is the path of the
item within the bucket.`,
		Description: `
It is not possible to atomically upload S3 objects exclusively when the target
does not already exist, therefore this cache is not suitable for deduplication.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("bucket", "The S3 bucket to store items in."),
			docs.FieldCommon("content_type", "The content type to set for each item."),
			docs.FieldAdvanced("force_path_style_urls", "Forces the client API to use path style URLs, which helps when connecting to custom endpoints."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on requests before abandoning it."),
			docs.FieldAdvanced("retries", "The maximum number of retry attempts to make before abandoning a request."),
		}.Merge(sess.FieldSpecs()),
	}
}

//------------------------------------------------------------------------------

// S3Config contains config fields for the S3 cache type.
type S3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string `json:"bucket" yaml:"bucket"`
	ForcePathStyleURLs bool   `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	ContentType        string `json:"content_type" yaml:"content_type"`
	Timeout            string `json:"timeout" yaml:"timeout"`
	Retries            int    `json:"retries" yaml:"retries"`
}

// NewS3Config creates a S3Config populated with default values.
func NewS3Config() S3Config {
	return S3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		ForcePathStyleURLs: false,
		ContentType:        "application/octet-stream",
		Timeout:            "5s",
		Retries:            3,
	}
}

//------------------------------------------------------------------------------

// S3 is a file system based cache implementation.
type S3 struct {
	session    *session.Session
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	s3         *s3.S3

	bucket      string
	timeout     time.Duration
	retries     int
	contentType string

	mLatency         metrics.StatTimer
	mGetCount        metrics.StatCounter
	mGetRetry        metrics.StatCounter
	mGetFailed       metrics.StatCounter
	mGetSuccess      metrics.StatCounter
	mGetLatency      metrics.StatTimer
	mGetNotFound     metrics.StatCounter
	mSetCount        metrics.StatCounter
	mSetRetry        metrics.StatCounter
	mSetFailed       metrics.StatCounter
	mSetSuccess      metrics.StatCounter
	mSetLatency      metrics.StatTimer
	mSetMultiCount   metrics.StatCounter
	mSetMultiRetry   metrics.StatCounter
	mSetMultiFailed  metrics.StatCounter
	mSetMultiSuccess metrics.StatCounter
	mSetMultiLatency metrics.StatTimer
	mAddCount        metrics.StatCounter
	mAddDupe         metrics.StatCounter
	mAddRetry        metrics.StatCounter
	mAddFailedDupe   metrics.StatCounter
	mAddFailedErr    metrics.StatCounter
	mAddSuccess      metrics.StatCounter
	mAddLatency      metrics.StatTimer
	mDelCount        metrics.StatCounter
	mDelRetry        metrics.StatCounter
	mDelFailedErr    metrics.StatCounter
	mDelSuccess      metrics.StatCounter
	mDelLatency      metrics.StatTimer
}

// NewAWSS3 creates a new S3 cache type.
func NewAWSS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return newS3(conf.AWSS3, mgr, log, stats)
}

// NewS3 creates a new S3 cache type.
func NewS3(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return newS3(conf.S3, mgr, log, stats)
}

func newS3(conf S3Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	timeout, err := time.ParseDuration(conf.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timeout: %v", err)
	}
	sess, err := conf.GetSession(func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(conf.ForcePathStyleURLs)
	})
	if err != nil {
		return nil, err
	}
	return &S3{
		session:    sess,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		s3:         s3.New(sess),

		bucket:      conf.Bucket,
		timeout:     timeout,
		retries:     conf.Retries,
		contentType: conf.ContentType,

		mLatency:         stats.GetTimer("latency"),
		mGetCount:        stats.GetCounter("get.count"),
		mGetRetry:        stats.GetCounter("get.retry"),
		mGetFailed:       stats.GetCounter("get.failed.error"),
		mGetNotFound:     stats.GetCounter("get.failed.not_found"),
		mGetSuccess:      stats.GetCounter("get.success"),
		mGetLatency:      stats.GetTimer("get.latency"),
		mSetCount:        stats.GetCounter("set.count"),
		mSetRetry:        stats.GetCounter("set.retry"),
		mSetFailed:       stats.GetCounter("set.failed.error"),
		mSetSuccess:      stats.GetCounter("set.success"),
		mSetLatency:      stats.GetTimer("set.latency"),
		mSetMultiCount:   stats.GetCounter("set_multi.count"),
		mSetMultiRetry:   stats.GetCounter("set_multi.retry"),
		mSetMultiFailed:  stats.GetCounter("set_multi.failed.error"),
		mSetMultiSuccess: stats.GetCounter("set_multi.success"),
		mSetMultiLatency: stats.GetTimer("set_multi.latency"),
		mAddCount:        stats.GetCounter("add.count"),
		mAddDupe:         stats.GetCounter("add.failed.duplicate"),
		mAddRetry:        stats.GetCounter("add.retry"),
		mAddFailedDupe:   stats.GetCounter("add.failed.duplicate"),
		mAddFailedErr:    stats.GetCounter("add.failed.error"),
		mAddSuccess:      stats.GetCounter("add.success"),
		mAddLatency:      stats.GetTimer("add.latency"),
		mDelCount:        stats.GetCounter("delete.count"),
		mDelRetry:        stats.GetCounter("delete.retry"),
		mDelFailedErr:    stats.GetCounter("delete.failed.error"),
		mDelSuccess:      stats.GetCounter("delete.success"),
		mDelLatency:      stats.GetTimer("delete.latency"),
	}, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (s *S3) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(
		aws.BackgroundContext(), s.timeout,
	)
	defer cancel()

	var err error
	var retries int
	var obj *s3.GetObjectOutput
	for {
		obj, err = s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
				return nil, types.ErrKeyNotFound
			}
		}
		retries++
		if err == nil || retries > s.retries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, err
		default:
		}
	}

	var bytes []byte
	if err == nil {
		bytes, err = io.ReadAll(obj.Body)
		obj.Body.Close()
	}
	return bytes, err
}

// Set attempts to set the value of a key.
func (s *S3) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(
		aws.BackgroundContext(), s.timeout,
	)
	defer cancel()

	var err error
	var retries int
	for {
		_, err = s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket:      &s.bucket,
			Key:         &key,
			Body:        bytes.NewReader(value),
			ContentType: &s.contentType,
		})
		retries++
		if err == nil || retries > s.retries {
			break
		}
		select {
		case <-ctx.Done():
			return err
		default:
		}
	}
	return err
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (s *S3) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := s.Set(k, v); err != nil {
			// TODO: Batch upload
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (s *S3) Add(key string, value []byte) error {
	_, err := s.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err == nil {
		return types.ErrKeyAlreadyExists
	}
	return s.Set(key, value)
}

// Delete attempts to remove a key.
func (s *S3) Delete(key string) error {
	ctx, cancel := context.WithTimeout(
		aws.BackgroundContext(), s.timeout,
	)
	defer cancel()

	var err error
	var retries int
	for {
		_, err = s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		})
		retries++
		if err == nil || retries > s.retries {
			break
		}
		select {
		case <-ctx.Done():
			return err
		default:
		}
	}
	return err
}

// CloseAsync shuts down the cache.
func (s *S3) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (s *S3) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
