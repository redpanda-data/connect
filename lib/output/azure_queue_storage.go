package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAzureQueueStorage] = TypeSpec{
		constructor: NewAzureQueueStorage,
		Status:      docs.StatusBeta,
		Summary: `
Sends messages to an Azure Queue Storage queue.`,
		Description: `
Only one authentication method is required, ` + "`storage_connection_string`" + ` or ` + "`storage_account` and `storage_access_key`" + `. If both are set then the ` + "`storage_connection_string`" + ` is given priority.

In order to set the ` + "`queue_name`" + ` you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.AzureQueueStorage, conf.AzureQueueStorage.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("storage_account", "The storage account to upload messages to. This field is ignored if `storage_connection_string` is set."),
			docs.FieldCommon("storage_access_key", "The storage account access key. This field is ignored if `storage_connection_string` is set."),
			docs.FieldCommon("storage_connection_string", "A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set."),
			docs.FieldCommon("queue_name", "The name of the target Queue Storage queue."),
			docs.FieldAdvanced(
				"ttl", "The TTL of each individual message as a duration string. Defaults to 0, meaning no retention period is set",
				"60s", "5m", "36h",
			),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// NewAzureQueueStorage creates a new AzureQueueStorage output type.
func NewAzureQueueStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := writer.NewAzureQueueStorage(conf.AzureQueueStorage, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.AzureQueueStorage.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeAzureQueueStorage, s, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeAzureQueueStorage, conf.AzureQueueStorage.MaxInFlight, s, log, stats,
		)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.AzureQueueStorage.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
