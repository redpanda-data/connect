package writer

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//------------------------------------------------------------------------------

// AmazonS3Config contains configuration fields for the AmazonS3 output type.
type AmazonS3Config struct {
	sess.Config             `json:",inline" yaml:",inline"`
	Bucket                  string                       `json:"bucket" yaml:"bucket"`
	ForcePathStyleURLs      bool                         `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	Path                    string                       `json:"path" yaml:"path"`
	Tags                    map[string]string            `json:"tags" yaml:"tags"`
	ContentType             string                       `json:"content_type" yaml:"content_type"`
	ContentEncoding         string                       `json:"content_encoding" yaml:"content_encoding"`
	CacheControl            string                       `json:"cache_control" yaml:"cache_control"`
	ContentDisposition      string                       `json:"content_disposition" yaml:"content_disposition"`
	ContentLanguage         string                       `json:"content_language" yaml:"content_language"`
	WebsiteRedirectLocation string                       `json:"website_redirect_location" yaml:"website_redirect_location"`
	Metadata                metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	StorageClass            string                       `json:"storage_class" yaml:"storage_class"`
	Timeout                 string                       `json:"timeout" yaml:"timeout"`
	KMSKeyID                string                       `json:"kms_key_id" yaml:"kms_key_id"`
	ServerSideEncryption    string                       `json:"server_side_encryption" yaml:"server_side_encryption"`
	MaxInFlight             int                          `json:"max_in_flight" yaml:"max_in_flight"`
	Batching                batch.PolicyConfig           `json:"batching" yaml:"batching"`
}

// NewAmazonS3Config creates a new Config with default values.
func NewAmazonS3Config() AmazonS3Config {
	return AmazonS3Config{
		Config:                  sess.NewConfig(),
		Bucket:                  "",
		ForcePathStyleURLs:      false,
		Path:                    `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		Tags:                    map[string]string{},
		ContentType:             "application/octet-stream",
		ContentEncoding:         "",
		CacheControl:            "",
		ContentDisposition:      "",
		ContentLanguage:         "",
		WebsiteRedirectLocation: "",
		Metadata:                metadata.NewExcludeFilterConfig(),
		StorageClass:            "STANDARD",
		Timeout:                 "5s",
		KMSKeyID:                "",
		ServerSideEncryption:    "",
		MaxInFlight:             1,
		Batching:                batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

type s3TagPair struct {
	key   string
	value *field.Expression
}

// AmazonS3 is a benthos writer.Type implementation that writes messages to an
// Amazon S3 bucket.
type AmazonS3 struct {
	conf AmazonS3Config

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

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3V2 creates a new Amazon S3 bucket writer.Type.
func NewAmazonS3V2(
	conf AmazonS3Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*AmazonS3, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	a := &AmazonS3{
		conf:    conf,
		log:     log,
		stats:   stats,
		timeout: timeout,
	}
	var err error
	if a.path, err = interop.NewBloblangField(mgr, conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if a.contentType, err = interop.NewBloblangField(mgr, conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content type expression: %v", err)
	}
	if a.contentEncoding, err = interop.NewBloblangField(mgr, conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content encoding expression: %v", err)
	}
	if a.cacheControl, err = interop.NewBloblangField(mgr, conf.CacheControl); err != nil {
		return nil, fmt.Errorf("failed to parse cache control expression: %v", err)
	}
	if a.contentDisposition, err = interop.NewBloblangField(mgr, conf.ContentDisposition); err != nil {
		return nil, fmt.Errorf("failed to parse content disposition expression: %v", err)
	}
	if a.contentLanguage, err = interop.NewBloblangField(mgr, conf.ContentLanguage); err != nil {
		return nil, fmt.Errorf("failed to parse content language expression: %v", err)
	}
	if a.websiteRedirectLocation, err = interop.NewBloblangField(mgr, conf.WebsiteRedirectLocation); err != nil {
		return nil, fmt.Errorf("failed to parse website redirect location expression: %v", err)
	}

	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	if a.storageClass, err = interop.NewBloblangField(mgr, conf.StorageClass); err != nil {
		return nil, fmt.Errorf("failed to parse storage class expression: %v", err)
	}

	a.tags = make([]s3TagPair, 0, len(conf.Tags))
	for k, v := range conf.Tags {
		vExpr, err := interop.NewBloblangField(mgr, v)
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

// ConnectWithContext attempts to establish a connection to the target S3
// bucket.
func (a *AmazonS3) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target S3 bucket.
func (a *AmazonS3) Connect() error {
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
	a.uploader = s3manager.NewUploader(sess)

	a.log.Infof("Uploading message parts as objects to Amazon S3 bucket: %v\n", a.conf.Bucket)
	return nil
}

// Write attempts to write message contents to a target S3 bucket as files.
func (a *AmazonS3) Write(msg *message.Batch) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target S3 bucket as
// files.
func (a *AmazonS3) WriteWithContext(wctx context.Context, msg *message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(
		wctx, a.timeout,
	)
	defer cancel()

	return IterateBatchedSend(msg, func(i int, p *message.Part) error {
		metadata := map[string]*string{}
		a.metaFilter.Iter(p, func(k, v string) error {
			metadata[k] = aws.String(v)
			return nil
		})

		var contentEncoding *string
		if ce := a.contentEncoding.String(i, msg); len(ce) > 0 {
			contentEncoding = aws.String(ce)
		}
		var cacheControl *string
		if ce := a.cacheControl.String(i, msg); len(ce) > 0 {
			cacheControl = aws.String(ce)
		}
		var contentDisposition *string
		if ce := a.contentDisposition.String(i, msg); len(ce) > 0 {
			contentDisposition = aws.String(ce)
		}
		var contentLanguage *string
		if ce := a.contentLanguage.String(i, msg); len(ce) > 0 {
			contentLanguage = aws.String(ce)
		}
		var websiteRedirectLocation *string
		if ce := a.websiteRedirectLocation.String(i, msg); len(ce) > 0 {
			websiteRedirectLocation = aws.String(ce)
		}

		uploadInput := &s3manager.UploadInput{
			Bucket:                  &a.conf.Bucket,
			Key:                     aws.String(a.path.String(i, msg)),
			Body:                    bytes.NewReader(p.Get()),
			ContentType:             aws.String(a.contentType.String(i, msg)),
			ContentEncoding:         contentEncoding,
			CacheControl:            cacheControl,
			ContentDisposition:      contentDisposition,
			ContentLanguage:         contentLanguage,
			WebsiteRedirectLocation: websiteRedirectLocation,
			StorageClass:            aws.String(a.storageClass.String(i, msg)),
			Metadata:                metadata,
		}

		// Prepare tags, escaping keys and values to ensure they're valid query string parameters.
		if len(a.tags) > 0 {
			tags := make([]string, len(a.tags))
			for j, pair := range a.tags {
				tags[j] = url.QueryEscape(pair.key) + "=" + url.QueryEscape(pair.value.String(i, msg))
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

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonS3) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonS3) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
