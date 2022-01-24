//go:build !wasm
// +build !wasm

package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/impl/azure"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AzureQueueStorage is a benthos writer.Type implementation that writes messages to an
// Azure Queue Storage queue.
type AzureQueueStorage struct {
	conf AzureQueueStorageConfig

	queueName  *field.Expression
	ttl        *field.Expression
	serviceURL *azqueue.ServiceURL

	log   log.Modular
	stats metrics.Type
}

// NewAzureQueueStorageV2 creates a new Azure Queue Storage writer type.
func NewAzureQueueStorageV2(
	conf AzureQueueStorageConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*AzureQueueStorage, error) {
	serviceURL, err := azure.GetQueueServiceURL(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, err
	}
	s := &AzureQueueStorage{
		conf:       conf,
		log:        log,
		stats:      stats,
		serviceURL: serviceURL,
	}

	if s.ttl, err = interop.NewBloblangField(mgr, conf.TTL); err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if s.queueName, err = interop.NewBloblangField(mgr, conf.QueueName); err != nil {
		return nil, fmt.Errorf("failed to parse table name expression: %v", err)
	}

	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target
// queue.
func (a *AzureQueueStorage) ConnectWithContext(ctx context.Context) error {
	return nil
}

// Connect attempts to establish a connection to the target
func (a *AzureQueueStorage) Connect() error {
	return nil
}

// Write attempts to write message contents to a target Azure Queue Storage queue.
func (a *AzureQueueStorage) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target Queue Storage
func (a *AzureQueueStorage) WriteWithContext(ctx context.Context, msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		queueURL := a.serviceURL.NewQueueURL(a.queueName.String(i, msg))
		msgURL := queueURL.NewMessagesURL()
		var ttl *time.Duration
		if ttls := a.ttl.String(i, msg); ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				a.log.Debugf("TTL must be a duration: %v\n", err)
				return err
			}
			ttl = &td
		}
		timeToLive := func() time.Duration {
			if ttl != nil {
				return *ttl
			}
			return 0
		}()
		message := string(p.Get())
		_, err := msgURL.Enqueue(ctx, message, 0, timeToLive)
		if err != nil {
			if cerr, ok := err.(azqueue.StorageError); ok {
				if cerr.ServiceCode() == azqueue.ServiceCodeQueueNotFound {
					ctx := context.Background()
					_, err = queueURL.Create(ctx, azqueue.Metadata{})
					if err != nil {
						return fmt.Errorf("error creating queue: %v", err)
					}
					_, err := msgURL.Enqueue(ctx, message, 0, 0)
					if err != nil {
						return fmt.Errorf("error retrying to enqueue message: %v", err)
					}
				} else {
					return fmt.Errorf("storage error message: %v", err)
				}
			} else {
				return fmt.Errorf("error enqueuing message: %v", err)
			}
		}
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AzureQueueStorage) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AzureQueueStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
