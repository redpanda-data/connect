package aws

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		sthree, err := newAmazonS3Writer(c.AWSS3, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("aws_s3", c.AWSS3.MaxInFlight, sthree, nm)
		if err != nil {
			return nil, err
		}
		return batcher.NewFromConfig(c.AWSS3.Batching, w, nm)
	}), docs.ComponentSpec{
		Name:    "aws_s3",
		Version: "3.36.0",
		Summary: `
Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the ` + "`path`" + ` field.`,
		Description: output.Description(true, false, `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

### Metadata

Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the [metadata docs](/docs/configuration/metadata).

### Tags

The tags field allows you to specify key/value pairs to attach to objects as tags, where the values support
[interpolation functions](/docs/configuration/interpolation#bloblang-queries):

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
    tags:
      Key1: Value1
      Timestamp: ${!meta("Timestamp")}
`+"```"+`

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).

### Batching

It's common to want to upload messages to S3 as batched archives, the easiest
way to do this is to batch your messages at the output level and join the batch
of messages with an
`+"[`archive`](/docs/components/processors/archive)"+` and/or
`+"[`compress`](/docs/components/processors/compress)"+` processor.

For example, if we wished to upload messages as a .tar.gz archive of documents
we could achieve that with the following config:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip
`+"```"+`

Alternatively, if we wished to upload JSON documents as a single large document
containing an array of objects we can do that with:

`+"```yaml"+`
output:
  aws_s3:
    bucket: TODO
    path: ${!count("files")}-${!timestamp_unix_nano()}.json
    batching:
      count: 100
      processors:
        - archive:
            format: json_array
`+"```"+``),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("bucket", "The bucket to upload messages to."),
			docs.FieldString(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.txt`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).IsInterpolated(),
			docs.FieldString(
				"tags", "Key/value pairs to store with the object as tags.",
				map[string]string{
					"Key1":      "Value1",
					"Timestamp": `${!meta("Timestamp")}`,
				},
			).IsInterpolated().Map(),
			docs.FieldString("content_type", "The content type to set for each object.").IsInterpolated(),
			docs.FieldString("content_encoding", "An optional content encoding to set for each object.").IsInterpolated().Advanced(),
			docs.FieldString("cache_control", "The cache control to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("content_disposition", "The content disposition to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("content_language", "The content language to set for each object.").Advanced().IsInterpolated(),
			docs.FieldString("website_redirect_location", "The website redirect location to set for each object.").Advanced().IsInterpolated(),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are attached to objects as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			docs.FieldString("storage_class", "The storage class to set for each object.").HasOptions(
				"STANDARD", "REDUCED_REDUNDANCY", "GLACIER", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "DEEP_ARCHIVE",
			).IsInterpolated().Advanced(),
			docs.FieldString("kms_key_id", "An optional server side encryption key.").Advanced(),
			docs.FieldString("server_side_encryption", "An optional server side encryption algorithm.").AtVersion("3.63.0").Advanced(),
			docs.FieldBool("force_path_style_urls", "Forces the client API to use path style URLs, which helps when connecting to custom endpoints.").Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldString("timeout", "The maximum period to wait on an upload before abandoning it and reattempting.").Advanced(),
			policy.FieldSpec(),
		).WithChildren(sess.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewAmazonS3Config()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

type s3TagPair struct {
	key   string
	value *field.Expression
}

type amazonS3Writer struct {
	conf output.AmazonS3Config

	path                    *field.Expression
	tags                    []s3TagPair
	contentType             *field.Expression
	contentEncoding         *field.Expression
	cacheControl            *field.Expression
	contentDisposition      *field.Expression
	contentLanguage         *field.Expression
	websiteRedirectLocation *field.Expression
	storageClass            *field.Expression
	metaFilter              *metadata.ExcludeFilter

	session  *session.Session
	uploader *s3manager.Uploader
	timeout  time.Duration

	log log.Modular
}

func newAmazonS3Writer(conf output.AmazonS3Config, mgr bundle.NewManagement) (*amazonS3Writer, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	a := &amazonS3Writer{
		conf:    conf,
		log:     mgr.Logger(),
		timeout: timeout,
	}
	var err error
	if a.path, err = mgr.BloblEnvironment().NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if a.contentType, err = mgr.BloblEnvironment().NewField(conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content type expression: %v", err)
	}
	if a.contentEncoding, err = mgr.BloblEnvironment().NewField(conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content encoding expression: %v", err)
	}
	if a.cacheControl, err = mgr.BloblEnvironment().NewField(conf.CacheControl); err != nil {
		return nil, fmt.Errorf("failed to parse cache control expression: %v", err)
	}
	if a.contentDisposition, err = mgr.BloblEnvironment().NewField(conf.ContentDisposition); err != nil {
		return nil, fmt.Errorf("failed to parse content disposition expression: %v", err)
	}
	if a.contentLanguage, err = mgr.BloblEnvironment().NewField(conf.ContentLanguage); err != nil {
		return nil, fmt.Errorf("failed to parse content language expression: %v", err)
	}
	if a.websiteRedirectLocation, err = mgr.BloblEnvironment().NewField(conf.WebsiteRedirectLocation); err != nil {
		return nil, fmt.Errorf("failed to parse website redirect location expression: %v", err)
	}

	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	if a.storageClass, err = mgr.BloblEnvironment().NewField(conf.StorageClass); err != nil {
		return nil, fmt.Errorf("failed to parse storage class expression: %v", err)
	}

	a.tags = make([]s3TagPair, 0, len(conf.Tags))
	for k, v := range conf.Tags {
		vExpr, err := mgr.BloblEnvironment().NewField(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tag expression for key '%v': %v", k, err)
		}
		a.tags = append(a.tags, s3TagPair{
			key:   k,
			value: vExpr,
		})
	}
	sort.Slice(a.tags, func(i, j int) bool {
		return a.tags[i].key < a.tags[j].key
	})

	return a, nil
}

func (a *amazonS3Writer) Connect(ctx context.Context) error {
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
	a.uploader = s3manager.NewUploader(sess)

	a.log.Infof("Uploading message parts as objects to Amazon S3 bucket: %v\n", a.conf.Bucket)
	return nil
}

func (a *amazonS3Writer) WriteBatch(wctx context.Context, msg message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(
		wctx, a.timeout,
	)
	defer cancel()

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		metadata := map[string]*string{}
		_ = a.metaFilter.Iter(p, func(k string, v any) error {
			metadata[k] = aws.String(query.IToString(v))
			return nil
		})

		var contentEncoding *string
		ce, err := a.contentEncoding.String(i, msg)
		if err != nil {
			return fmt.Errorf("content encoding interpolation: %w", err)
		}
		if len(ce) > 0 {
			contentEncoding = aws.String(ce)
		}
		var cacheControl *string
		if ce, err = a.cacheControl.String(i, msg); err != nil {
			return fmt.Errorf("cache control interpolation: %w", err)
		}
		if len(ce) > 0 {
			cacheControl = aws.String(ce)
		}
		var contentDisposition *string
		if ce, err = a.contentDisposition.String(i, msg); err != nil {
			return fmt.Errorf("content disposition interpolation: %w", err)
		}
		if len(ce) > 0 {
			contentDisposition = aws.String(ce)
		}
		var contentLanguage *string
		if ce, err = a.contentLanguage.String(i, msg); err != nil {
			return fmt.Errorf("content language interpolation: %w", err)
		}
		if len(ce) > 0 {
			contentLanguage = aws.String(ce)
		}
		var websiteRedirectLocation *string
		if ce, err = a.websiteRedirectLocation.String(i, msg); err != nil {
			return fmt.Errorf("website redirect location interpolation: %w", err)
		}
		if len(ce) > 0 {
			websiteRedirectLocation = aws.String(ce)
		}

		key, err := a.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("key interpolation: %w", err)
		}

		contentType, err := a.contentType.String(i, msg)
		if err != nil {
			return fmt.Errorf("content type interpolation: %w", err)
		}

		storageClass, err := a.storageClass.String(i, msg)
		if err != nil {
			return fmt.Errorf("storage class interpolation: %w", err)
		}

		uploadInput := &s3manager.UploadInput{
			Bucket:                  &a.conf.Bucket,
			Key:                     aws.String(key),
			Body:                    bytes.NewReader(p.AsBytes()),
			ContentType:             aws.String(contentType),
			ContentEncoding:         contentEncoding,
			CacheControl:            cacheControl,
			ContentDisposition:      contentDisposition,
			ContentLanguage:         contentLanguage,
			WebsiteRedirectLocation: websiteRedirectLocation,
			StorageClass:            aws.String(storageClass),
			Metadata:                metadata,
		}

		// Prepare tags, escaping keys and values to ensure they're valid query string parameters.
		if len(a.tags) > 0 {
			tags := make([]string, len(a.tags))
			for j, pair := range a.tags {
				tagStr, err := pair.value.String(i, msg)
				if err != nil {
					return fmt.Errorf("tag %v interpolation: %w", pair.key, err)
				}
				tags[j] = url.QueryEscape(pair.key) + "=" + url.QueryEscape(tagStr)
			}
			uploadInput.Tagging = aws.String(strings.Join(tags, "&"))
		}

		if a.conf.KMSKeyID != "" {
			uploadInput.ServerSideEncryption = aws.String("aws:kms")
			uploadInput.SSEKMSKeyId = &a.conf.KMSKeyID
		}

		// NOTE: This overrides the ServerSideEncryption set above. We need this to preserve
		// backwards compatibility, where it is allowed to only set kms_key_id in the config and
		// the ServerSideEncryption value of "aws:kms" is implied.
		if a.conf.ServerSideEncryption != "" {
			uploadInput.ServerSideEncryption = &a.conf.ServerSideEncryption
		}

		if _, err := a.uploader.UploadWithContext(ctx, uploadInput); err != nil {
			return err
		}
		return nil
	})
}

func (a *amazonS3Writer) Close(context.Context) error {
	return nil
}
