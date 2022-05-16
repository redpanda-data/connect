package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

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
	Batching                batchconfig.Config           `json:"batching" yaml:"batching"`
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
		MaxInFlight:             64,
		Batching:                batchconfig.NewConfig(),
	}
}
