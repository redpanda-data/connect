package input

import (
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
)

// AWSS3SQSConfig contains configuration for hooking up the S3 input with an SQS queue.
type AWSS3SQSConfig struct {
	URL          string `json:"url" yaml:"url"`
	Endpoint     string `json:"endpoint" yaml:"endpoint"`
	EnvelopePath string `json:"envelope_path" yaml:"envelope_path"`
	KeyPath      string `json:"key_path" yaml:"key_path"`
	BucketPath   string `json:"bucket_path" yaml:"bucket_path"`
	DelayPeriod  string `json:"delay_period" yaml:"delay_period"`
	MaxMessages  int64  `json:"max_messages" yaml:"max_messages"`
}

// NewAWSS3SQSConfig creates a new AWSS3SQSConfig with default values.
func NewAWSS3SQSConfig() AWSS3SQSConfig {
	return AWSS3SQSConfig{
		URL:          "",
		Endpoint:     "",
		EnvelopePath: "",
		KeyPath:      "Records.*.s3.object.key",
		BucketPath:   "Records.*.s3.bucket.name",
		DelayPeriod:  "",
		MaxMessages:  10,
	}
}

// AWSS3Config contains configuration values for the aws_s3 input type.
type AWSS3Config struct {
	sess.Config        `json:",inline" yaml:",inline"`
	Bucket             string         `json:"bucket" yaml:"bucket"`
	Codec              string         `json:"codec" yaml:"codec"`
	MaxBuffer          int            `json:"max_buffer" yaml:"max_buffer"`
	Prefix             string         `json:"prefix" yaml:"prefix"`
	ForcePathStyleURLs bool           `json:"force_path_style_urls" yaml:"force_path_style_urls"`
	DeleteObjects      bool           `json:"delete_objects" yaml:"delete_objects"`
	SQS                AWSS3SQSConfig `json:"sqs" yaml:"sqs"`
}

// NewAWSS3Config creates a new AWSS3Config with default values.
func NewAWSS3Config() AWSS3Config {
	return AWSS3Config{
		Config:             sess.NewConfig(),
		Bucket:             "",
		Prefix:             "",
		Codec:              "all-bytes",
		MaxBuffer:          1000000,
		ForcePathStyleURLs: false,
		DeleteObjects:      false,
		SQS:                NewAWSS3SQSConfig(),
	}
}
