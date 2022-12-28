package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		r, err := newAzureQueueStorage(conf.AzureQueueStorage, nm, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("azure_queue_storage", r, nm)
	}), docs.ComponentSpec{
		Name:    "azure_queue_storage",
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
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"storage_account",
				"The storage account to dequeue messages from. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_sas_token",
				"The storage account SAS token. This field is ignored if `storage_connection_string` or `storage_access_key` are set.",
			),
			docs.FieldString(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` / `storage_sas_token` are not set.",
			),
			docs.FieldString(
				"queue_name", "The name of the source storage queue.", "foo_queue", `${! env("MESSAGE_TYPE").lowercase() }`,
			).IsInterpolated(),
			docs.FieldString(
				"dequeue_visibility_timeout", "The timeout duration until a dequeued message gets visible again, 30s by default",
			).AtVersion("3.45.0").Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of unprocessed messages to fetch at a given time.").Advanced(),
			docs.FieldBool("track_properties", "If set to `true` the queue is polled on each read request for information such as the queue message lag. These properties are added to consumed messages as metadata, but will also have a negative performance impact.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewAzureQueueStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
	}
}

type azureQueueStorage struct {
	conf input.AzureQueueStorageConfig

	queueName                *field.Expression
	serviceURL               *azqueue.ServiceURL
	dequeueVisibilityTimeout time.Duration

	log   log.Modular
	stats metrics.Type
}

func newAzureQueueStorage(conf input.AzureQueueStorageConfig, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (*azureQueueStorage, error) {
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

func (a *azureQueueStorage) Connect(ctx context.Context) error {
	return nil
}

func (a *azureQueueStorage) ReadBatch(ctx context.Context) (msg message.Batch, ackFn input.AsyncAckFn, err error) {
	var queueName string
	if queueName, err = a.queueName.String(0, msg); err != nil {
		err = fmt.Errorf("queue name interpolation error: %w", err)
		return
	}
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
			part.MetaSetMut("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
			part.MetaSetMut("queue_storage_queue_name", queueName)
			if a.conf.TrackProperties {
				msgLag := 0
				if approxMsgCount >= n {
					msgLag = int(approxMsgCount - n)
				}
				part.MetaSetMut("queue_storage_message_lag", msgLag)
			}
			for k, v := range metadata {
				part.MetaSetMut(k, v)
			}
			msg = append(msg, part)
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

func (a *azureQueueStorage) Close(ctx context.Context) error {
	return nil
}
