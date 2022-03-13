package input

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/shared"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
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

// AzureQueueStorage is a benthos reader.Type implementation that reads messages
// from an Azure Queue Storage container.
type azureQueueStorage struct {
	conf AzureQueueStorageConfig

	queueName                *field.Expression
	serviceURL               *azqueue.ServiceURL
	dequeueVisibilityTimeout time.Duration

	log   log.Modular
	stats metrics.Type
}

// newAzureQueueStorage creates a new Azure Storage Queue input type.
func newAzureQueueStorage(conf AzureQueueStorageConfig, mgr interop.Manager, log log.Modular, stats metrics.Type) (*azureQueueStorage, error) {
	serviceURL, err := shared.GetQueueServiceURL(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, err
	}

	a := &azureQueueStorage{
		conf:       conf,
		log:        log,
		stats:      stats,
		serviceURL: serviceURL,
	}

	if a.queueName, err = mgr.BloblEnvironment().NewField(conf.QueueName); err != nil {
		return nil, fmt.Errorf("failed to parse queue name expression: %v", err)
	}

	if len(conf.DequeueVisibilityTimeout) > 0 {
		var err error
		if a.dequeueVisibilityTimeout, err = time.ParseDuration(conf.DequeueVisibilityTimeout); err != nil {
			return nil, fmt.Errorf("unable to parse dequeue visibility timeout duration string: %w", err)
		}
	}

	return a, nil
}

// ConnectWithContext attempts to establish a connection
func (a *azureQueueStorage) ConnectWithContext(ctx context.Context) error {
	return nil
}

// ReadWithContext attempts to read a new message from the target Azure Storage Queue Storage container.
func (a *azureQueueStorage) ReadWithContext(ctx context.Context) (msg *message.Batch, ackFn reader.AsyncAckFn, err error) {
	queueName := a.queueName.String(0, msg)
	queueURL := a.serviceURL.NewQueueURL(queueName)
	messageURL := queueURL.NewMessagesURL()
	var approxMsgCount int32
	if a.conf.TrackProperties {
		if props, err := queueURL.GetProperties(ctx); err == nil {
			approxMsgCount = props.ApproximateMessagesCount()
		}
	}
	dequeue, err := messageURL.Dequeue(ctx, a.conf.MaxInFlight, a.dequeueVisibilityTimeout)
	if err != nil {
		if cerr, ok := err.(azqueue.StorageError); ok {
			if cerr.ServiceCode() == azqueue.ServiceCodeQueueNotFound {
				ctx := context.Background()
				_, err = queueURL.Create(ctx, azqueue.Metadata{})
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("storage error message: %v", cerr)
		}
		return nil, nil, fmt.Errorf("error dequeing message: %v", err)
	}
	if n := dequeue.NumMessages(); n > 0 {
		props, _ := queueURL.GetProperties(ctx)
		metadata := props.NewMetadata()
		msg := message.QuickBatch(nil)
		dqm := make([]*azqueue.DequeuedMessage, n)
		for i := int32(0); i < n; i++ {
			queueMsg := dequeue.Message(i)
			part := message.NewPart([]byte(queueMsg.Text))
			part.MetaSet("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
			part.MetaSet("queue_storage_queue_name", queueName)
			if a.conf.TrackProperties {
				msgLag := 0
				if approxMsgCount >= n {
					msgLag = int(approxMsgCount - n)
				}
				part.MetaSet("queue_storage_message_lag", strconv.Itoa(msgLag))
			}
			for k, v := range metadata {
				part.MetaSet(k, v)
			}
			msg.Append(part)
			dqm[i] = queueMsg
		}
		return msg, func(ctx context.Context, res error) error {
			for i := int32(0); i < n; i++ {
				msgIDURL := messageURL.NewMessageIDURL(dqm[i].ID)
				_, err = msgIDURL.Delete(ctx, dqm[i].PopReceipt)
				if err != nil {
					return fmt.Errorf("error deleting message: %v", err)
				}
			}
			return nil
		}, nil
	}
	return nil, nil, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *azureQueueStorage) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *azureQueueStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
