package output

import (
	"strings"

	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"google.golang.org/api/googleapi"
)

const (
	overwriteMode     = "overwrite"
	appendMode        = "append"
	errorIfExistsMode = "errorifexists"
	ignoreMode        = "ignoreifexists"
)

// GCPCloudStorageConfig contains configuration fields for the GCP Cloud Storage
// output type.
type GCPCloudStorageConfig struct {
	Bucket          string             `json:"bucket" yaml:"bucket"`
	Path            string             `json:"path" yaml:"path"`
	ContentType     string             `json:"content_type" yaml:"content_type"`
	ContentEncoding string             `json:"content_encoding" yaml:"content_encoding"`
	ChunkSize       int                `json:"chunk_size" yaml:"chunk_size"`
	MaxInFlight     int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
	Mode            string             `json:"mode" yaml:"mode"`
}

// NewGCPCloudStorageConfig creates a new Config with default values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Bucket:          "",
		Path:            `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		ChunkSize:       googleapi.DefaultUploadChunkSize,
		MaxInFlight:     1,
		Batching:        batch.NewPolicyConfig(),
		Mode:            "overwrite",
	}
}

// IsOverwriteMode returns true if mode is overwrite
func (g GCPCloudStorageConfig) IsOverwriteMode() bool {
	return strings.EqualFold(g.Mode, overwriteMode)
}

// IsAppendMode returns true if mode is append
func (g GCPCloudStorageConfig) IsAppendMode() bool {
	return strings.EqualFold(g.Mode, appendMode)
}

// IsErrorIfExistsMode returns true if mode is error if exists
func (g GCPCloudStorageConfig) IsErrorIfExistsMode() bool {
	return strings.EqualFold(g.Mode, errorIfExistsMode)
}

// IsIgnoreIfExistsMode returns true if mode is ignore if exists
func (g GCPCloudStorageConfig) IsIgnoreIfExistsMode() bool {
	return strings.EqualFold(g.Mode, ignoreMode)
}
