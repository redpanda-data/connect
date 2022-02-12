package input

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func init() {
	Constructors[TypeAzureQueueStorage] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
			r, err := newAzureQueueStorage(conf.AzureQueueStorage, mgr, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(TypeAzureQueueStorage, false, r, log, stats)
		}),
		Status:  docs.StatusBeta,
		Version: "3.42.0",
		Summary: `
Dequeue objects from an Azure Storage Queue.`,
		Description: `
Dequeue objects from an Azure Storage Queue.

This input adds the following metadata fields to each message:

` + "```" + `
- queue_storage_insertion_time
- queue_storage_queue_name
- queue_storage_message_lag (if 'track_properties' set to true)
- All user defined queue metadata
` + "```" + `

Only one authentication method is required, ` + "`storage_connection_string`" + ` or ` + "`storage_account` and `storage_access_key`" + `. If both are set then the ` + "`storage_connection_string`" + ` is given priority.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"storage_account",
				"The storage account to dequeue messages from. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_sas_token",
				"The storage account SAS token. This field is ignored if `storage_connection_string` or `storage_access_key` are set.",
			),
			docs.FieldCommon(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` / `storage_sas_token` are not set.",
			),
			docs.FieldCommon(
				"queue_name", "The name of the source storage queue.", "foo_queue", `${! env("MESSAGE_TYPE").lowercase() }`,
			).IsInterpolated(),
			docs.FieldAdvanced(
				"dequeue_visibility_timeout", "The timeout duration until a dequeued message gets visible again, 30s by default",
			).AtVersion("3.45.0"),
			docs.FieldAdvanced("max_in_flight", "The maximum number of unprocessed messages to fetch at a given time."),
			docs.FieldAdvanced("track_properties", "If set to `true` the queue is polled on each read request for information such as the queue message lag. These properties are added to consumed messages as metadata, but will also have a negative performance impact."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// AzureQueueStorageConfig contains configuration fields for the AzureQueueStorage
// input type.
type AzureQueueStorageConfig struct {
	StorageAccount           string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey         string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageSASToken          string `json:"storage_sas_token" yaml:"storage_sas_token"`
	StorageConnectionString  string `json:"storage_connection_string" yaml:"storage_connection_string"`
	QueueName                string `json:"queue_name" yaml:"queue_name"`
	DequeueVisibilityTimeout string `json:"dequeue_visibility_timeout" yaml:"dequeue_visibility_timeout"`
	MaxInFlight              int32  `json:"max_in_flight" yaml:"max_in_flight"`
	TrackProperties          bool   `json:"track_properties" yaml:"track_properties"`
}

// NewAzureQueueStorageConfig creates a new AzureQueueStorageConfig with default
// values.
func NewAzureQueueStorageConfig() AzureQueueStorageConfig {
	return AzureQueueStorageConfig{
		DequeueVisibilityTimeout: "30s",
		MaxInFlight:              10,
		TrackProperties:          false,
	}
}
