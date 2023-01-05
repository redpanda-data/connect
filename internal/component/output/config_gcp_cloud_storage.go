package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
)

const (
	// GCPCloudStorageErrorIfExistsCollisionMode - error-if-exists.
	GCPCloudStorageErrorIfExistsCollisionMode = "error-if-exists"

	// GCPCloudStorageAppendCollisionMode - append.
	GCPCloudStorageAppendCollisionMode = "append"

	// GCPCloudStorageIgnoreCollisionMode - ignore.
	GCPCloudStorageIgnoreCollisionMode = "ignore"

	// GCPCloudStorageOverwriteCollisionMode - overwrite.
	GCPCloudStorageOverwriteCollisionMode = "overwrite"
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
	Batching        batchconfig.Config `json:"batching" yaml:"batching"`
	CollisionMode   string             `json:"collision_mode" yaml:"collision_mode"`
}

// NewGCPCloudStorageConfig creates a new Config with default values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Bucket:          "",
		Path:            `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		ChunkSize:       16 * 1024 * 1024, // googleapi.DefaultUploadChunkSize
		MaxInFlight:     64,
		Batching:        batchconfig.NewConfig(),
		CollisionMode:   GCPCloudStorageOverwriteCollisionMode,
	}
}
