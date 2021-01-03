// +build !wasm

package writer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AzureQueueStorage is a benthos writer.Type implementation that writes messages to an
// Azure Queue Storage queue.
type AzureQueueStorage struct {
	conf AzureQueueStorageConfig

	queueName  field.Expression
	ttl        field.Expression
	serviceURL *azqueue.ServiceURL

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

// NewAzureQueueStorage creates a new Azure Queue Storage writer type.
func NewAzureQueueStorage(
	conf AzureQueueStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*AzureQueueStorage, error) {
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
	serviceURL := azqueue.NewServiceURL(*endpoint, p)

	s := &AzureQueueStorage{
		conf:       conf,
		log:        log,
		stats:      stats,
		serviceURL: &serviceURL,
	}

	if s.ttl, err = bloblang.NewField(conf.TTL); err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if s.queueName, err = bloblang.NewField(conf.QueueName); err != nil {
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
	return msg.Iter(func(i int, p types.Part) error {
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
						return fmt.Errorf("error retrying to enqueu message: %v", err)
					}
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
