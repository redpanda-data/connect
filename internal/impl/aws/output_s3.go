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

package aws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	// S3 Output Fields
	s3oFieldBucket                  = "bucket"
	s3oFieldForcePathStyleURLs      = "force_path_style_urls"
	s3oFieldPath                    = "path"
	s3oFieldLocalFilePath           = "local_file_path"
	s3oFieldTags                    = "tags"
	s3oFieldChecksumAlgorithm       = "checksum_algorithm"
	s3oFieldContentType             = "content_type"
	s3oFieldContentEncoding         = "content_encoding"
	s3oFieldCacheControl            = "cache_control"
	s3oFieldContentDisposition      = "content_disposition"
	s3oFieldContentLanguage         = "content_language"
	s3oFieldContentMD5              = "content_md5"
	s3oFieldWebsiteRedirectLocation = "website_redirect_location"
	s3oFieldMetadata                = "metadata"
	s3oFieldStorageClass            = "storage_class"
	s3oFieldTimeout                 = "timeout"
	s3oFieldKMSKeyID                = "kms_key_id"
	s3oFieldServerSideEncryption    = "server_side_encryption"
	s3oFieldObjectCannedACL         = "object_canned_acl"
	s3oFieldBatching                = "batching"
)

type s3TagPair struct {
	key   string
	value *service.InterpolatedString
}

type s3oConfig struct {
	Bucket string

	Path                    *service.InterpolatedString
	LocalFilePath           *service.InterpolatedString
	Tags                    []s3TagPair
	ContentType             *service.InterpolatedString
	ContentEncoding         *service.InterpolatedString
	CacheControl            *service.InterpolatedString
	ChecksumAlgorithm       string
	ContentDisposition      *service.InterpolatedString
	ContentLanguage         *service.InterpolatedString
	ContentMD5              *service.InterpolatedString
	WebsiteRedirectLocation *service.InterpolatedString
	Metadata                *service.MetadataExcludeFilter
	StorageClass            *service.InterpolatedString
	Timeout                 time.Duration
	KMSKeyID                string
	ServerSideEncryption    string
	UsePathStyle            bool
	ObjectCannedACL         types.ObjectCannedACL

	aconf aws.Config
}

func s3oConfigFromParsed(pConf *service.ParsedConfig) (conf s3oConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(s3oFieldBucket); err != nil {
		return
	}

	if conf.UsePathStyle, err = pConf.FieldBool(s3oFieldForcePathStyleURLs); err != nil {
		return
	}

	if conf.Path, err = pConf.FieldInterpolatedString(s3oFieldPath); err != nil {
		return
	}

	if conf.LocalFilePath, err = pConf.FieldInterpolatedString(s3oFieldLocalFilePath); err != nil {
		return
	}

	var tagMap map[string]*service.InterpolatedString
	if tagMap, err = pConf.FieldInterpolatedStringMap(s3oFieldTags); err != nil {
		return
	}

	conf.Tags = make([]s3TagPair, 0, len(tagMap))
	for k, v := range tagMap {
		conf.Tags = append(conf.Tags, s3TagPair{key: k, value: v})
	}
	sort.Slice(conf.Tags, func(i, j int) bool {
		return conf.Tags[i].key < conf.Tags[j].key
	})

	if conf.ContentType, err = pConf.FieldInterpolatedString(s3oFieldContentType); err != nil {
		return
	}
	if conf.ContentEncoding, err = pConf.FieldInterpolatedString(s3oFieldContentEncoding); err != nil {
		return
	}
	if conf.CacheControl, err = pConf.FieldInterpolatedString(s3oFieldCacheControl); err != nil {
		return
	}
	if conf.ContentDisposition, err = pConf.FieldInterpolatedString(s3oFieldContentDisposition); err != nil {
		return
	}
	if conf.ContentLanguage, err = pConf.FieldInterpolatedString(s3oFieldContentLanguage); err != nil {
		return
	}
	if conf.ContentMD5, err = pConf.FieldInterpolatedString(s3oFieldContentMD5); err != nil {
		return
	}
	if conf.ChecksumAlgorithm, err = pConf.FieldString(s3oFieldChecksumAlgorithm); err != nil {
		return
	}
	if conf.WebsiteRedirectLocation, err = pConf.FieldInterpolatedString(s3oFieldWebsiteRedirectLocation); err != nil {
		return
	}
	if conf.Metadata, err = pConf.FieldMetadataExcludeFilter(s3oFieldMetadata); err != nil {
		return
	}
	if conf.StorageClass, err = pConf.FieldInterpolatedString(s3oFieldStorageClass); err != nil {
		return
	}
	if conf.Timeout, err = pConf.FieldDuration(s3oFieldTimeout); err != nil {
		return
	}
	if conf.KMSKeyID, err = pConf.FieldString(s3oFieldKMSKeyID); err != nil {
		return
	}
	if conf.ServerSideEncryption, err = pConf.FieldString(s3oFieldServerSideEncryption); err != nil {
		return
	}

	var objectCannedACL string
	if objectCannedACL, err = pConf.FieldString(s3oFieldObjectCannedACL); err != nil {
		return
	}

	if slices.Contains(types.ObjectCannedACL("").Values(), types.ObjectCannedACL(objectCannedACL)) {
		conf.ObjectCannedACL = types.ObjectCannedACL(objectCannedACL)
	} else {
		err = fmt.Errorf("invalid object canned ACL value: %v", objectCannedACL)
		return
	}

	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	return
}

func s3oOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded with the path specified with the `+"`path`"+` field.`).
		Description(`
In order to have a different path for each object you should use function interpolations described in xref:configuration:interpolation.adoc#bloblang-queries[Bloblang queries], which are calculated per message of a batch.

== Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the xref:configuration:metadata.adoc[metadata docs].

== Tags

The tags field allows you to specify key/value pairs to attach to objects as tags, where the values support xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions]:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!counter()}-${!timestamp_unix_nano()}.tar.gz
    tags:
      Key1: Value1
      Timestamp: ${!meta("Timestamp")}
`+"```"+`

=== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[].

== Batching

It's common to want to upload messages to S3 as batched archives, the easiest way to do this is to batch your messages at the output level and join the batch of messages with an `+"xref:components:processors/archive.adoc[`archive`]"+` and/or `+"xref:components:processors/compress.adoc[`compress`]"+` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents we could achieve that with the following config:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!counter()}-${!timestamp_unix_nano()}.tar.gz
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip
`+"```"+`

Alternatively, if we wished to upload JSON documents as a single large document containing an array of objects we can do that with:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!counter()}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
`+"```"+``+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewStringField(s3oFieldBucket).
				Description("The bucket to upload messages to."),
			service.NewInterpolatedStringField(s3oFieldPath).
				Description("The path of each message to upload.").
				Default(`${!counter()}-${!timestamp_unix_nano()}.txt`).
				Example(`${!counter()}-${!timestamp_unix_nano()}.txt`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`),
			service.NewInterpolatedStringField(s3oFieldLocalFilePath).
				Description("The path of the local file to upload.").
				Default(``).
				Example(`/tmp/file.json`),
			service.NewInterpolatedStringMapField(s3oFieldTags).
				Description("Key/value pairs to store with the object as tags.").
				Default(map[string]any{}).
				Example(map[string]any{
					"Key1":      "Value1",
					"Timestamp": `${!meta("Timestamp")}`,
				}),
			service.NewInterpolatedStringField(s3oFieldContentType).
				Description("The content type to set for each object.").
				Default("application/octet-stream"),
			service.NewInterpolatedStringField(s3oFieldContentEncoding).
				Description("An optional content encoding to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldCacheControl).
				Description("The cache control to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldContentDisposition).
				Description("The content disposition to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldContentLanguage).
				Description("The content language to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldContentMD5).
				Description("The content MD5 to set for each object.").
				Default("").
				Advanced(),
			service.NewInterpolatedStringField(s3oFieldWebsiteRedirectLocation).
				Description("The website redirect location to set for each object.").
				Default("").
				Advanced(),
			service.NewMetadataExcludeFilterField(s3oFieldMetadata).
				Description("Specify criteria for which metadata values are attached to objects as headers."),
			service.NewInterpolatedStringEnumField(s3oFieldStorageClass,
				"STANDARD", "REDUCED_REDUNDANCY", "GLACIER", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "DEEP_ARCHIVE",
			).
				Description("The storage class to set for each object.").
				Default("STANDARD").
				Advanced(),
			service.NewStringField(s3oFieldKMSKeyID).
				Description("An optional server side encryption key.").
				Default("").
				Advanced(),
			service.NewStringEnumField(s3oFieldChecksumAlgorithm,
				"CRC32", "CRC32C", "SHA1", "SHA256",
			).
				Description("The algorithm used to create the checksum for each object.").
				Default("").
				Advanced(),
			service.NewStringField(s3oFieldServerSideEncryption).
				Description("An optional server side encryption algorithm.").
				Version("3.63.0").
				Default("").
				Advanced(),
			service.NewBoolField(s3oFieldForcePathStyleURLs).
				Description("Forces the client API to use path style URLs, which helps when connecting to custom endpoints.").
				Advanced().
				Default(false),
			service.NewOutputMaxInFlightField(),
			service.NewDurationField(s3oFieldTimeout).
				Description("The maximum period to wait on an upload before abandoning it and reattempting.").
				Advanced().
				Default("5s"),
			service.NewStringEnumField(s3oFieldObjectCannedACL,
				slices.Collect(func(yield func(string) bool) {
					for _, v := range types.ObjectCannedACL("").Values() {
						if !yield(string(v)) {
							return
						}
					}
				})...).
				Description("The object canned ACL value.").
				Default(string(types.ObjectCannedACLPrivate)).
				Advanced(),
			service.NewBatchPolicyField(s3oFieldBatching),
		).
		Fields(config.SessionFields()...)
}

func init() {
	service.MustRegisterBatchOutput("aws_s3", s3oOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(koFieldBatching); err != nil {
				return
			}
			var wConf s3oConfig
			if wConf, err = s3oConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newAmazonS3Writer(wConf, mgr)
			return
		})
}

type amazonS3Writer struct {
	conf     s3oConfig
	uploader *manager.Uploader
	log      *service.Logger
}

func newAmazonS3Writer(conf s3oConfig, mgr *service.Resources) (*amazonS3Writer, error) {
	a := &amazonS3Writer{
		conf: conf,
		log:  mgr.Logger(),
	}
	return a, nil
}

// ConnectionTest attempts to test the connection configuration of this output
// without actually sending data. The connection, if successful, is then
// closed.
func (a *amazonS3Writer) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	client := s3.NewFromConfig(a.conf.aconf, func(o *s3.Options) {
		o.UsePathStyle = a.conf.UsePathStyle
		if a.conf.aconf.BaseEndpoint != nil {
			o.BaseEndpoint = a.conf.aconf.BaseEndpoint
		}
	})

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(a.conf.Bucket),
	})
	if err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("failed to access bucket %s: %w", a.conf.Bucket, err)).AsList()
	}
	return service.ConnectionTestSucceeded().AsList()
}

func (a *amazonS3Writer) Connect(context.Context) error {
	if a.uploader != nil {
		return nil
	}

	client := s3.NewFromConfig(a.conf.aconf, func(o *s3.Options) {
		o.UsePathStyle = a.conf.UsePathStyle

		// For S3-compatible services, set BaseEndpoint at the client level
		if a.conf.aconf.BaseEndpoint != nil {
			o.BaseEndpoint = a.conf.aconf.BaseEndpoint
		}
	})
	a.uploader = manager.NewUploader(client)
	return nil
}

func (a *amazonS3Writer) WriteBatch(wctx context.Context, msg service.MessageBatch) error {
	if a.uploader == nil {
		return service.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.conf.Timeout)
	defer cancel()

	return msg.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		metadata := map[string]string{}
		_ = a.conf.Metadata.WalkMut(m, func(k string, v any) error {
			metadata[k] = bloblang.ValueToString(v)
			return nil
		})

		var contentEncoding *string
		ce, err := msg.TryInterpolatedString(i, a.conf.ContentEncoding)
		if err != nil {
			return fmt.Errorf("content encoding interpolation: %w", err)
		}
		if ce != "" {
			contentEncoding = aws.String(ce)
		}
		var cacheControl *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.CacheControl); err != nil {
			return fmt.Errorf("cache control interpolation: %w", err)
		}
		if ce != "" {
			cacheControl = aws.String(ce)
		}
		var contentDisposition *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.ContentDisposition); err != nil {
			return fmt.Errorf("content disposition interpolation: %w", err)
		}
		if ce != "" {
			contentDisposition = aws.String(ce)
		}
		var contentLanguage *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.ContentLanguage); err != nil {
			return fmt.Errorf("content language interpolation: %w", err)
		}
		if ce != "" {
			contentLanguage = aws.String(ce)
		}
		var contentMD5 *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.ContentMD5); err != nil {
			return fmt.Errorf("content MD5 interpolation: %w", err)
		}
		if ce != "" {
			contentMD5 = aws.String(ce)
		}
		var websiteRedirectLocation *string
		if ce, err = msg.TryInterpolatedString(i, a.conf.WebsiteRedirectLocation); err != nil {
			return fmt.Errorf("website redirect location interpolation: %w", err)
		}
		if ce != "" {
			websiteRedirectLocation = aws.String(ce)
		}

		key, err := msg.TryInterpolatedString(i, a.conf.Path)
		if err != nil {
			return fmt.Errorf("key interpolation: %w", err)
		}

		contentType, err := msg.TryInterpolatedString(i, a.conf.ContentType)
		if err != nil {
			return fmt.Errorf("content type interpolation: %w", err)
		}

		storageClass, err := msg.TryInterpolatedString(i, a.conf.StorageClass)
		if err != nil {
			return fmt.Errorf("storage class interpolation: %w", err)
		}

		uploadBody, err := a.getUploadBody(m)
		if err != nil {
			return err
		}

		uploadInput := &s3.PutObjectInput{
			Bucket:                  &a.conf.Bucket,
			Key:                     aws.String(key),
			Body:                    uploadBody,
			ContentType:             aws.String(contentType),
			ContentEncoding:         contentEncoding,
			CacheControl:            cacheControl,
			ContentDisposition:      contentDisposition,
			ContentLanguage:         contentLanguage,
			ContentMD5:              contentMD5,
			WebsiteRedirectLocation: websiteRedirectLocation,
			StorageClass:            types.StorageClass(storageClass),
			Metadata:                metadata,
			ACL:                     a.conf.ObjectCannedACL,
		}

		// Prepare tags, escaping keys and values to ensure they're valid query string parameters.
		if len(a.conf.Tags) > 0 {
			tags := make([]string, len(a.conf.Tags))
			for j, pair := range a.conf.Tags {
				tagStr, err := msg.TryInterpolatedString(i, pair.value)
				if err != nil {
					return fmt.Errorf("tag %v interpolation: %w", pair.key, err)
				}
				tags[j] = url.QueryEscape(pair.key) + "=" + url.QueryEscape(tagStr)
			}
			uploadInput.Tagging = aws.String(strings.Join(tags, "&"))
		}

		if a.conf.KMSKeyID != "" {
			uploadInput.ServerSideEncryption = types.ServerSideEncryptionAwsKms
			uploadInput.SSEKMSKeyId = &a.conf.KMSKeyID
		}

		if a.conf.ChecksumAlgorithm != "" {
			uploadInput.ChecksumAlgorithm = types.ChecksumAlgorithm(a.conf.ChecksumAlgorithm)
		}

		// NOTE: This overrides the ServerSideEncryption set above. We need this to preserve
		// backwards compatibility, where it is allowed to only set kms_key_id in the config and
		// the ServerSideEncryption value of "aws:kms" is implied.
		if a.conf.ServerSideEncryption != "" {
			uploadInput.ServerSideEncryption = types.ServerSideEncryption(a.conf.ServerSideEncryption)
		}

		if _, err := a.uploader.Upload(ctx, uploadInput); err != nil {
			return err
		}
		return nil
	})
}

func (a *amazonS3Writer) getUploadBody(m *service.Message) (io.Reader, error) {
	localFilePath, err := a.conf.LocalFilePath.TryString(m)
	if err != nil {
		return nil, fmt.Errorf("local file path interpolation error: %w", err)
	}

	if localFilePath != "" {
		file, err := os.Open(localFilePath)
		if err != nil {
			return nil, fmt.Errorf("local file read error: %w", err)
		}
		return file, nil
	}

	mBytes, err := m.AsBytes()
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(mBytes), nil
}

func (*amazonS3Writer) Close(context.Context) error {
	return nil
}
