package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAzureQueueStorage] = TypeSpec{
		constructor: fromSimpleConstructor(NewAzureQueueStorage),
		Status:      docs.StatusBeta,
		Version:     "3.36.0",
		Summary: `
Sends messages to an Azure Storage Queue.`,
		Description: `
Only one authentication method is required, ` + "`storage_connection_string`" + ` or ` + "`storage_account` and `storage_access_key`" + `. If both are set then the ` + "`storage_connection_string`" + ` is given priority.

In order to set the ` + "`queue_name`" + ` you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are calculated per message of a batch.`,
		Async:   true,
		Batches: true,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("storage_account", "The storage account to upload messages to. This field is ignored if `storage_connection_string` is set."),
			docs.FieldString("storage_access_key", "The storage account access key. This field is ignored if `storage_connection_string` is set."),
			docs.FieldString("storage_connection_string", "A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set."),
			docs.FieldString("queue_name", "The name of the target Queue Storage queue.").IsInterpolated(),
			docs.FieldString(
				"ttl", "The TTL of each individual message as a duration string. Defaults to 0, meaning no retention period is set",
				"60s", "5m", "36h",
			).IsInterpolated().Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").AtVersion("3.45.0"),
			policy.FieldSpec(),
		),
		Categories: []string{
			"Services",
			"Azure",
		},
	}
}

//------------------------------------------------------------------------------

// NewAzureQueueStorage creates a new AzureQueueStorage output type.
func NewAzureQueueStorage(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	s, err := writer.NewAzureQueueStorageV2(conf.AzureQueueStorage, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeAzureQueueStorage, conf.AzureQueueStorage.MaxInFlight, s, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.AzureQueueStorage.Batching, OnlySinglePayloads(w), mgr, log, stats)
}

//------------------------------------------------------------------------------
