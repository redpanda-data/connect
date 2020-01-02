package writer

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//------------------------------------------------------------------------------

// AmazonS3Config contains configuration fields for the AmazonS3 output type.
type AmazonS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string `json:"bucket" yaml:"bucket"`
	ForcePathStyleURLs bool   `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	Path               string `json:"path" yaml:"path"`
	ContentType        string `json:"content_type" yaml:"content_type"`
	ContentEncoding    string `json:"content_encoding" yaml:"content_encoding"`
	Timeout            string `json:"timeout" yaml:"timeout"`
	KMSKeyID           string `json:"kms_key_id" yaml:"kms_key_id"`
	MaxInFlight        int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewAmazonS3Config creates a new Config with default values.
func NewAmazonS3Config() AmazonS3Config {
	return AmazonS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		ForcePathStyleURLs: false,
		Path:               "${!count:files}-${!timestamp_unix_nano}.txt",
		ContentType:        "application/octet-stream",
		ContentEncoding:    "",
		Timeout:            "5s",
		KMSKeyID:           "",
		MaxInFlight:        1,
	}
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos writer.Type implementation that writes messages to an
// Amazon S3 bucket.
type AmazonS3 struct {
	conf AmazonS3Config

	path            *text.InterpolatedString
	contentType     *text.InterpolatedString
	contentEncoding *text.InterpolatedString

	session  *session.Session
	uploader *s3manager.Uploader
	timeout  time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3 creates a new Amazon S3 bucket writer.Type.
func NewAmazonS3(
	conf AmazonS3Config,
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
	return &AmazonS3{
		conf:            conf,
		log:             log,
		stats:           stats,
		path:            text.NewInterpolatedString(conf.Path),
		contentType:     text.NewInterpolatedString(conf.ContentType),
		contentEncoding: text.NewInterpolatedString(conf.ContentEncoding),
		timeout:         timeout,
	}, nil
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
func (a *AmazonS3) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target S3 bucket as
// files.
func (a *AmazonS3) WriteWithContext(wctx context.Context, msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(
		wctx, a.timeout,
	)
	defer cancel()

	return msg.Iter(func(i int, p types.Part) error {
		metadata := map[string]*string{}
		p.Metadata().Iter(func(k, v string) error {
			metadata[k] = aws.String(v)
			return nil
		})

		lMsg := message.Lock(msg, i)

		var contentEncoding *string
		if ce := a.contentEncoding.Get(lMsg); len(ce) > 0 {
			contentEncoding = aws.String(ce)
		}

		uploadInput := &s3manager.UploadInput{
			Bucket:          &a.conf.Bucket,
			Key:             aws.String(a.path.Get(lMsg)),
			Body:            bytes.NewReader(p.Get()),
			ContentType:     aws.String(a.contentType.Get(lMsg)),
			ContentEncoding: contentEncoding,
			Metadata:        metadata,
		}

		if a.conf.KMSKeyID != "" {
			uploadInput.ServerSideEncryption = aws.String("aws:kms")
			uploadInput.SSEKMSKeyId = &a.conf.KMSKeyID
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
