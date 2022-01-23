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
	Constructors[TypeAzureBlobStorage] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newAzureBlobStorage(conf.AzureBlobStorage, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeAzureBlobStorage,
				true,
				reader.NewAsyncPreserver(r),
				log, stats,
			)
		}),
		Status:  docs.StatusBeta,
		Version: "3.36.0",
		Summary: `
Downloads objects within an Azure Blob Storage container, optionally filtered by
a prefix.`,
		Description: `
Downloads objects within an Azure Blob Storage container, optionally filtered by a prefix.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- blob_storage_key
- blob_storage_container
- blob_storage_last_modified
- blob_storage_last_modified_unix
- blob_storage_content_type
- blob_storage_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"storage_account",
				"The storage account to download blobs from. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_sas_token",
				"The storage account SAS token. This field is ignored if `storage_connection_string` or `storage_access_key` are set.",
			).AtVersion("3.38.0"),
			docs.FieldCommon(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` / `storage_sas_token` are not set.",
			),
			docs.FieldCommon(
				"container", "The name of the container from which to download blobs.",
			),
			docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the blob once they are processed."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage
// input type.
type AzureBlobStorageConfig struct {
	StorageAccount          string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageSASToken         string `json:"storage_sas_token" yaml:"storage_sas_token"`
	StorageConnectionString string `json:"storage_connection_string" yaml:"storage_connection_string"`
	Container               string `json:"container" yaml:"container"`
	Prefix                  string `json:"prefix" yaml:"prefix"`
	Codec                   string `json:"codec" yaml:"codec"`
	DeleteObjects           bool   `json:"delete_objects" yaml:"delete_objects"`
}

// NewAzureBlobStorageConfig creates a new AzureBlobStorageConfig with default
// values.
func NewAzureBlobStorageConfig() AzureBlobStorageConfig {
	return AzureBlobStorageConfig{
		Codec: "all-bytes",
	}
}
