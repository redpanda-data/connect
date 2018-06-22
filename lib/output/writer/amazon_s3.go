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

package writer

import (
	"bytes"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//------------------------------------------------------------------------------

// AmazonAWSCredentialsConfig contains configuration params for AWS credentials.
type AmazonAWSCredentialsConfig struct {
	ID     string `json:"id" yaml:"id"`
	Secret string `json:"secret" yaml:"secret"`
	Token  string `json:"token" yaml:"token"`
	Role   string `json:"role" yaml:"role"`
}

// AmazonS3Config is configuration values for the input type.
type AmazonS3Config struct {
	Region      string                     `json:"region" yaml:"region"`
	Bucket      string                     `json:"bucket" yaml:"bucket"`
	Path        string                     `json:"path" yaml:"path"`
	Credentials AmazonAWSCredentialsConfig `json:"credentials" yaml:"credentials"`
	TimeoutS    int64                      `json:"timeout_s" yaml:"timeout_s"`
}

// NewAmazonS3Config creates a new Config with default values.
func NewAmazonS3Config() AmazonS3Config {
	return AmazonS3Config{
		Region: "eu-west-1",
		Bucket: "",
		Path:   "${!count:files}-${!timestamp_unix_nano}.txt",
		Credentials: AmazonAWSCredentialsConfig{
			ID:     "",
			Secret: "",
			Token:  "",
		},
		TimeoutS: 5,
	}
}

//------------------------------------------------------------------------------

// AmazonS3 is a benthos writer.Type implementation that writes messages to an
// Amazon S3 bucket.
type AmazonS3 struct {
	conf AmazonS3Config

	pathBytes       []byte
	interpolatePath bool

	session  *session.Session
	uploader *s3manager.Uploader

	log   log.Modular
	stats metrics.Type
}

// NewAmazonS3 creates a new Amazon S3 bucket writer.Type.
func NewAmazonS3(
	conf AmazonS3Config,
	log log.Modular,
	stats metrics.Type,
) *AmazonS3 {
	pathBytes := []byte(conf.Path)
	interpolatePath := text.ContainsFunctionVariables(pathBytes)
	return &AmazonS3{
		conf:            conf,
		pathBytes:       pathBytes,
		interpolatePath: interpolatePath,
		log:             log.NewModule(".output.amazon_s3"),
		stats:           stats,
	}
}

// Connect attempts to establish a connection to the target S3 bucket and any
// relevant queues used to traverse the objects (SQS, etc).
func (a *AmazonS3) Connect() error {
	if a.session != nil {
		return nil
	}

	awsConf := aws.NewConfig()
	if len(a.conf.Region) > 0 {
		awsConf = awsConf.WithRegion(a.conf.Region)
	}
	if len(a.conf.Credentials.ID) > 0 {
		awsConf = awsConf.WithCredentials(credentials.NewStaticCredentials(
			a.conf.Credentials.ID,
			a.conf.Credentials.Secret,
			a.conf.Credentials.Token,
		))
	}

	sess, err := session.NewSession(awsConf)
	if err != nil {
		return err
	}

	if len(a.conf.Credentials.Role) > 0 {
		sess.Config = sess.Config.WithCredentials(
			stscreds.NewCredentials(sess, a.conf.Credentials.Role),
		)
	}

	a.session = sess
	a.uploader = s3manager.NewUploader(sess)

	a.log.Infof("Uploading message parts as objects to Amazon S3 bucket: %v\n", a.conf.Bucket)
	return nil
}

// Write attempts to write message contents to a target S3 bucket as files.
func (a *AmazonS3) Write(msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	for _, part := range msg.GetAll() {
		path := a.conf.Path
		if a.interpolatePath {
			path = string(text.ReplaceFunctionVariables(a.pathBytes))
		}

		if _, err := a.uploader.Upload(&s3manager.UploadInput{
			Body:   bytes.NewReader(part),
			Bucket: aws.String(a.conf.Bucket),
			Key:    aws.String(path),
		}); err != nil {
			return err
		}
	}

	return nil
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
