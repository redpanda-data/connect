package aws

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/public/service"
)

func s3CacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Second
	retriesDefaults.MaxInterval = time.Second * 5
	retriesDefaults.MaxElapsedTime = time.Second * 30

	spec := service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Summary(`Stores each item in an S3 bucket as a file, where an item ID is the path of the item within the bucket.`).
		Description(`It is not possible to atomically upload S3 objects exclusively when the target does not already exist, therefore this cache is not suitable for deduplication.`).
		Field(service.NewStringField("bucket").
			Description("The S3 bucket to store items in.")).
		Field(service.NewStringField("content_type").
			Description("The content type to set for each item.").
			Default("application/octet-stream")).
		Field(service.NewBoolField("force_path_style_urls").
			Description("Forces the client API to use path style URLs, which helps when connecting to custom endpoints.").
			Advanced().
			Default(false)).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced())

	for _, f := range config.SessionFields() {
		spec = spec.Field(f)
	}
	return spec
}

func init() {
	err := service.RegisterCache(
		"aws_s3", s3CacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			s, err := newS3CacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			return s, nil
		})
	if err != nil {
		panic(err)
	}
}

func newS3CacheFromConfig(conf *service.ParsedConfig) (*s3Cache, error) {
	bucket, err := conf.FieldString("bucket")
	if err != nil {
		return nil, err
	}
	contentType, err := conf.FieldString("content_type")
	if err != nil {
		return nil, err
	}
	forcePathStyleURLs, err := conf.FieldBool("force_path_style_urls")
	if err != nil {
		return nil, err
	}

	sess, err := GetSession(conf, func(c *aws.Config) {
		c.S3ForcePathStyle = aws.Bool(forcePathStyleURLs)
	})
	if err != nil {
		return nil, err
	}
	client := s3.New(sess)

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}

	return newS3Cache(bucket, contentType, backOff, client), nil
}

//------------------------------------------------------------------------------

type s3Cache struct {
	s3 s3iface.S3API

	bucket      string
	contentType string

	boffPool sync.Pool
}

func newS3Cache(bucket, contentType string, backOff *backoff.ExponentialBackOff, s3 s3iface.S3API) *s3Cache {
	return &s3Cache{
		s3: s3,

		bucket:      bucket,
		contentType: contentType,

		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}
}

//------------------------------------------------------------------------------

func (s *s3Cache) Get(ctx context.Context, key string) (body []byte, err error) {
	boff := s.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		s.boffPool.Put(boff)
	}()

	var obj *s3.GetObjectOutput
	for {
		if obj, err = s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		}); err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
				err = service.ErrKeyNotFound
				return
			}
		} else {
			body, err = io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			return
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

// Set attempts to set the value of a key.
func (s *s3Cache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) (err error) {
	boff := s.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		s.boffPool.Put(boff)
	}()

	for {
		if _, err = s.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket:      &s.bucket,
			Key:         &key,
			Body:        bytes.NewReader(value),
			ContentType: &s.contentType,
		}); err == nil {
			return
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

func (s *s3Cache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	if _, err := s.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}); err == nil {
		return service.ErrKeyAlreadyExists
	}
	return s.Set(ctx, key, value, nil)
}

func (s *s3Cache) Delete(ctx context.Context, key string) (err error) {
	boff := s.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		s.boffPool.Put(boff)
	}()

	for {
		if _, err = s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		}); err == nil {
			return
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

func (s *s3Cache) Close(context.Context) error {
	return nil
}
