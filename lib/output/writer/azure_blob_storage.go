//go:build !wasm
// +build !wasm

package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AzureBlobStorage is a benthos writer. Type implementation that writes messages to an
// Azure Blob Storage storage account.
type AzureBlobStorage struct {
	conf        AzureBlobStorageConfig
	container   *field.Expression
	path        *field.Expression
	blobType    *field.Expression
	accessLevel *field.Expression
	client      storage.BlobStorageClient
	log         log.Modular
	stats       metrics.Type
}

// NewAzureBlobStorageV2 creates a new AzureBlobStorage writer.Type.
func NewAzureBlobStorageV2(
	mgr types.Manager,
	conf AzureBlobStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*AzureBlobStorage, error) {
	if conf.StorageAccount == "" && conf.StorageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var client storage.Client
	var err error
	if len(conf.StorageConnectionString) > 0 {
		if strings.Contains(conf.StorageConnectionString, "UseDevelopmentStorage=true;") {
			client, err = storage.NewEmulatorClient()
		} else {
			client, err = storage.NewClientFromConnectionString(conf.StorageConnectionString)
		}
	} else if len(conf.StorageAccessKey) > 0 {
		client, err = storage.NewBasicClient(conf.StorageAccount, conf.StorageAccessKey)
	} else {
		// The SAS token in the Azure UI is provided as an URL query string with
		// the '?' prepended to it which confuses url.ParseQuery
		token, err := url.ParseQuery(strings.TrimPrefix(conf.StorageSASToken, "?"))
		if err != nil {
			return nil, fmt.Errorf("invalid azure storage SAS token: %v", err)
		}
		client = storage.NewAccountSASClient(conf.StorageAccount, token, azure.PublicCloud)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	a := &AzureBlobStorage{
		conf:   conf,
		log:    log,
		stats:  stats,
		client: client.GetBlobService(),
	}
	if a.container, err = interop.NewBloblangField(mgr, conf.Container); err != nil {
		return nil, fmt.Errorf("failed to parse container expression: %v", err)
	}
	if a.path, err = interop.NewBloblangField(mgr, conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if a.blobType, err = interop.NewBloblangField(mgr, conf.BlobType); err != nil {
		return nil, fmt.Errorf("failed to parse blob type expression: %v", err)
	}
	if a.accessLevel, err = interop.NewBloblangField(mgr, conf.PublicAccessLevel); err != nil {
		return nil, fmt.Errorf("failed to parse public access level expression: %v", err)
	}
	return a, nil
}

// ConnectWithContext attempts to establish a connection to the target Blob Storage Account.
func (a *AzureBlobStorage) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target Blob Storage Account.
func (a *AzureBlobStorage) Connect() error {
	return nil
}

// Write attempts to write message contents to a target Azure Blob Storage container as files.
func (a *AzureBlobStorage) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

func (a *AzureBlobStorage) uploadBlob(b *storage.Blob, blobType string, message []byte) error {
	if blobType == "APPEND" {
		exists, err := b.Exists()
		if err != nil {
			return err
		}
		if !exists {
			if err := b.PutAppendBlob(nil); err != nil {
				return err
			}
		}
		return b.AppendBlock(message, nil)
	}
	return b.CreateBlockBlobFromReader(bytes.NewReader(message), nil)
}

func (a *AzureBlobStorage) createContainer(c *storage.Container, accessLevel string) error {
	opts := storage.CreateContainerOptions{}
	switch accessLevel {
	case "BLOB":
		opts.Access = storage.ContainerAccessTypeBlob
	case "CONTAINER":
		opts.Access = storage.ContainerAccessTypeContainer
	}
	return c.Create(&opts)
}

// WriteWithContext attempts to write message contents to a target storage account as files.
func (a *AzureBlobStorage) WriteWithContext(_ context.Context, msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		c := a.client.GetContainerReference(a.container.String(i, msg))
		b := c.GetBlobReference(a.path.String(i, msg))
		if err := a.uploadBlob(b, a.blobType.String(i, msg), p.Get()); err != nil {
			if containerNotFound(err) {
				if cerr := a.createContainer(c, a.accessLevel.String(i, msg)); cerr != nil {
					a.log.Debugf("error creating container: %v.", cerr)
					return cerr
				}
				err = a.uploadBlob(b, a.blobType.String(i, msg), p.Get())
				if err != nil {
					a.log.Debugf("error retrying to upload  blob: %v.", err)
				}
			}
			return err
		}
		return nil
	})
}

func containerNotFound(err error) bool {
	if serr, ok := err.(storage.AzureStorageServiceError); ok {
		return serr.Code == "ContainerNotFound"
	}
	return false
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AzureBlobStorage) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AzureBlobStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
