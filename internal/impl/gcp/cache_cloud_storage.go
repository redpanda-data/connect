package gcp

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"

	"github.com/benthosdev/benthos/v4/public/service"
)

func gcpCloudStorageCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Summary(`Use a Google Cloud Storage bucket as a cache.`).
		Description(`It is not possible to atomically upload cloud storage objects exclusively when the target does not already exist, therefore this cache is not suitable for deduplication.`).
		Field(service.NewStringField("bucket").
			Description("The Google Cloud Storage bucket to store items in."))

	return spec
}

func init() {
	err := service.RegisterCache(
		"gcp_cloud_storage", gcpCloudStorageCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newGcpCloudStorageCacheFromConfig(conf)
		})

	if err != nil {
		panic(err)
	}
}

func newGcpCloudStorageCacheFromConfig(parsedConf *service.ParsedConfig) (*gcpCloudStorageCache, error) {
	bucket, err := parsedConf.FieldString("bucket")
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &gcpCloudStorageCache{
		bucketHandle: client.Bucket(bucket),
	}, nil
}

//------------------------------------------------------------------------------

type gcpCloudStorageCache struct {
	bucketHandle *storage.BucketHandle
}

func (c *gcpCloudStorageCache) Get(ctx context.Context, key string) ([]byte, error) {
	reader, err := c.bucketHandle.Object(key).NewReader(ctx)
	if err != nil {
		// Check if the object does not exist and return the proper error
		if err == storage.ErrObjectNotExist {
			return nil, service.ErrKeyNotFound
		}
		return nil, err
	}

	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *gcpCloudStorageCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	writer := c.bucketHandle.Object(key).NewWriter(ctx)

	_, err := writer.Write(value)
	if err != nil {
		return err
	}

	return writer.Close()
}

func (c *gcpCloudStorageCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	objectHandle := c.bucketHandle.Object(key)

	// Check if the object already exists
	_, err := objectHandle.Attrs(ctx)
	if err == nil {
		return service.ErrKeyAlreadyExists
	}

	writer := objectHandle.NewWriter(ctx)

	_, err = writer.Write(value)
	if err != nil {
		return err
	}

	return writer.Close()
}

func (c *gcpCloudStorageCache) Delete(ctx context.Context, key string) error {
	return c.bucketHandle.Object(key).Delete(ctx)
}

func (c *gcpCloudStorageCache) Close(ctx context.Context) error {
	return nil
}
