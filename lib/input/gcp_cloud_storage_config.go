package input

import (
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	Constructors[TypeGCPCloudStorage] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newGCPCloudStorage(conf.GCPCloudStorage, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeGCPCloudStorage,
				true,
				reader.NewAsyncBundleUnacks(
					reader.NewAsyncPreserver(r),
				),
				log, stats,
			)
		}),
		Status:  docs.StatusExperimental,
		Version: "3.41.1",
		Summary: `
Downloads objects within a Google Cloud Storage bucket, optionally filtered by a prefix.`,
		Description: `
Downloads objects within a Google Cloud Storage bucket, optionally filtered by a prefix.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- gcs_key
- gcs_bucket
- gcs_last_modified
- gcs_last_modified_unix
- gcs_content_type
- gcs_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).

### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/gcp.md).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"bucket", "The name of the bucket from which to download objects.",
			),
			docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the bucket once they are processed."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryGCP,
		},
	}
}

//------------------------------------------------------------------------------

// GCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage input type.
type GCPCloudStorageConfig struct {
	Bucket        string `json:"bucket" yaml:"bucket"`
	Prefix        string `json:"prefix" yaml:"prefix"`
	Codec         string `json:"codec" yaml:"codec"`
	DeleteObjects bool   `json:"delete_objects" yaml:"delete_objects"`
}

// NewGCPCloudStorageConfig creates a new GCPCloudStorageConfig with default
// values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Codec: "all-bytes",
	}
}
