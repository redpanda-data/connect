// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcp

import (
	"context"
	"errors"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func gcpCloudStorageCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use a Google Cloud Storage bucket as a cache.`).
		Description(`It is not possible to atomically upload cloud storage objects exclusively when the target does not already exist, therefore this cache is not suitable for deduplication.`).
		Field(service.NewStringField("bucket").
			Description("The Google Cloud Storage bucket to store items in.")).
		Field(service.NewStringField("content_type").
			Description("Optional field to explicitly set the Content-Type.").Optional()).
		Field(service.NewStringField("credentials_json").
			Description("An optional field to set Google Service Account Credentials json.").Secret().Default(""))

	return spec
}

func init() {
	service.MustRegisterCache(
		"gcp_cloud_storage", gcpCloudStorageCacheConfig(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Cache, error) {
			return newGcpCloudStorageCacheFromConfig(conf)
		})
}

func newGcpCloudStorageCacheFromConfig(parsedConf *service.ParsedConfig) (*gcpCloudStorageCache, error) {
	bucket, err := parsedConf.FieldString("bucket")
	if err != nil {
		return nil, err
	}

	contentType := ""
	if parsedConf.Contains("content_type") {
		contentType, err = parsedConf.FieldString("content_type")
		if err != nil {
			return nil, err
		}
	}

	var opt []option.ClientOption
	if parsedConf.Contains("credentials_json") {
		credsJSON, err := parsedConf.FieldString("credentials_json")
		if err != nil {
			return nil, err
		}
		opt, err = getClientOptionWithCredential(credsJSON, opt)
		if err != nil {
			return nil, err
		}
	}

	client, err := storage.NewClient(context.Background(), opt...)
	if err != nil {
		return nil, err
	}

	return &gcpCloudStorageCache{
		bucketHandle: client.Bucket(bucket),
		contentType:  contentType,
	}, nil
}

//------------------------------------------------------------------------------

type gcpCloudStorageCache struct {
	bucketHandle *storage.BucketHandle
	contentType  string
}

func (c *gcpCloudStorageCache) Get(ctx context.Context, key string) ([]byte, error) {
	reader, err := c.bucketHandle.Object(key).NewReader(ctx)
	if err != nil {
		// Check if the object does not exist and return the proper error
		if errors.Is(err, storage.ErrObjectNotExist) {
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

	if c.contentType != "" {
		writer.ContentType = c.contentType
	}

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

	if c.contentType != "" {
		writer.ContentType = c.contentType
	}

	_, err = writer.Write(value)
	if err != nil {
		return err
	}

	return writer.Close()
}

func (c *gcpCloudStorageCache) Delete(ctx context.Context, key string) error {
	return c.bucketHandle.Object(key).Delete(ctx)
}

func (*gcpCloudStorageCache) Close(context.Context) error {
	return nil
}
