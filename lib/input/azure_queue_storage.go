// +build !wasm

package input

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/Jeffail/benthos/v3/lib/message"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AzureQueueStorage is a benthos reader.Type implementation that reads messages
// from an Azure Queue Storage container.
type azureQueueStorage struct {
	conf AzureQueueStorageConfig

	queueURL *azqueue.QueueURL

	log   log.Modular
	stats metrics.Type
}

const (
	azQueueEndpointExp  = "https://%s.queue.core.windows.net"
	devQueueEndpointExp = "http://localhost:10001/%s"
	azAccountName       = "accountname"
	azAccountKey        = "accountkey"
	devAccountName      = "devstoreaccount1"
	devAccountKey       = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// parseConnectionString extracts the credentials from the connection string.
func parseConnectionString(input string) (storageAccount, storageAccessKey string, err error) {
	// build a map of connection string key/value pairs
	parts := map[string]string{}
	for _, pair := range strings.Split(input, ";") {
		if pair == "" {
			continue
		}
		equalDex := strings.IndexByte(pair, '=')
		if equalDex <= 0 {
			fmt.Println(fmt.Errorf("invalid connection segment %q", pair))
		}
		value := strings.TrimSpace(pair[equalDex+1:])
		key := strings.TrimSpace(strings.ToLower(pair[:equalDex]))
		parts[key] = value
	}
	accountName, ok := parts[azAccountName]
	if !ok {
		return "", "", errors.New("invalid connection string")
	}
	accountKey, ok := parts[azAccountKey]
	if !ok {
		return "", "", errors.New("invalid connection string")
	}
	return accountName, accountKey, nil
}

// newAzureQueueStorage creates a new Azure Storage Queue input type.
func newAzureQueueStorage(conf AzureQueueStorageConfig, log log.Modular, stats metrics.Type) (*azureQueueStorage, error) {
	var err error
	if len(conf.StorageAccount) == 0 && len(conf.StorageConnectionString) == 0 {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var storageAccount string
	var storageAccessKey string
	var endpointExp = azQueueEndpointExp
	if len(conf.StorageConnectionString) > 0 {
		storageAccount, storageAccessKey, err = parseConnectionString(conf.StorageConnectionString)
		if err != nil {
			return nil, err
		}
		if strings.Contains(conf.StorageConnectionString, "UseDevelopmentStorage=true;") {
			storageAccount = devAccountName
			storageAccessKey = devAccountKey
			endpointExp = devQueueEndpointExp
		} else if strings.Contains(conf.StorageConnectionString, devAccountName) {
			endpointExp = devQueueEndpointExp
		}
	} else {
		storageAccount = conf.StorageAccount
		storageAccessKey = conf.StorageAccessKey
	}
	if len(storageAccount) == 0 {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	var credential azqueue.Credential
	if len(storageAccessKey) > 0 {
		credential, _ = azqueue.NewSharedKeyCredential(storageAccount, storageAccessKey)
	} else {
		credential = azqueue.NewAnonymousCredential()
	}
	p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{})
	endpoint, _ := url.Parse(fmt.Sprintf(endpointExp, storageAccount))
	queueURL := azqueue.NewServiceURL(*endpoint, p).NewQueueURL(conf.QueueName)
	a := &azureQueueStorage{
		conf:     conf,
		log:      log,
		stats:    stats,
		queueURL: &queueURL,
	}
	return a, nil
}

// ConnectWithContext attempts to establish a connection
func (a *azureQueueStorage) ConnectWithContext(ctx context.Context) error {
	return nil
}

// ReadWithContext attempts to read a new message from the target Azure Storage Queue
// Storage container.
func (a *azureQueueStorage) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	messageURL := a.queueURL.NewMessagesURL()
	dequeue, err := messageURL.Dequeue(ctx, 1, 30*time.Second)
	if err != nil {
		if cerr, ok := err.(azqueue.StorageError); ok {
			if cerr.ServiceCode() == azqueue.ServiceCodeQueueNotFound {
				ctx := context.Background()
				_, err = a.queueURL.Create(ctx, azqueue.Metadata{})
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("storage error message: %v", cerr)
		}
		return nil, nil, fmt.Errorf("error dequeing message: %v", err)
	}
	if n := dequeue.NumMessages(); n > 0 {
		props, _ := a.queueURL.GetProperties(ctx)
		metadata := props.NewMetadata()
		msg := message.New(nil)
		for m := int32(0); m < dequeue.NumMessages(); m++ {
			queueMsg := dequeue.Message(m)
			part := message.NewPart([]byte(queueMsg.Text))
			msg.Append(part)
			meta := msg.Get(0).Metadata()
			meta.Set("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
			for k, v := range metadata {
				meta.Set(k, v)
			}
			msgIDURL := messageURL.NewMessageIDURL(queueMsg.ID)
			_, err = msgIDURL.Delete(ctx, queueMsg.PopReceipt)
		}
		return msg, func(rctx context.Context, res types.Response) error {
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
