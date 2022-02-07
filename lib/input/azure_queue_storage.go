//go:build !wasm
// +build !wasm

package input

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/Jeffail/benthos/v3/internal/impl/azure"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

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
func newAzureQueueStorage(conf AzureQueueStorageConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*azureQueueStorage, error) {
	serviceURL, err := azure.GetQueueServiceURL(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, err
	}

	a := &azureQueueStorage{
		conf:       conf,
		log:        log,
		stats:      stats,
		serviceURL: serviceURL,
	}

	if a.queueName, err = interop.NewBloblangField(mgr, conf.QueueName); err != nil {
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
		return msg, func(ctx context.Context, res types.Response) error {
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
