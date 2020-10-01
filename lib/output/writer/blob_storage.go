// +build !wasm

package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AzureBlobStorage is a benthos writer. Type implementation that writes messages to an
// Azure Blob Storage storage account.
type AzureBlobStorage struct {
	conf             AzureBlobStorageConfig
	credential       azblob.Credential
	container        field.Expression
	path             field.Expression
	blobType         field.Expression
	timeout          time.Duration
	log              log.Modular
	stats            metrics.Type
	storageURLFormat string
}

// NewAzureBlobStorage creates a new Amazon S3 bucket writer.Type.
func NewAzureBlobStorage(
	conf AzureBlobStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*AzureBlobStorage, error) {
	var timeout time.Duration
	var err error
	if tout := conf.Timeout; len(tout) > 0 {
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}

	var credential azblob.Credential
	storageAccount := conf.StorageAccount
	storageAccessKey := conf.StorageAccessKey
	storageURLFormat := ""
	if len(conf.StorageConnectionString) != 0 {
		pcs := extractValues(conf.StorageConnectionString)
		if sa := pcs["AccountName"]; sa == "" {
			return nil, errors.New("invalid azure storage connection string (missing AccountName)")
		} else if sk := pcs["AccountKey"]; sk == "" {
			return nil, errors.New("invalid azure storage connection string (missing AccountKey)")
		} else {
			storageAccount = sa
			storageAccessKey = sk
			if eps := pcs["EndpointSuffix"]; eps != "" {
				storageURLFormat = fmt.Sprintf("%s://%s.blob.%s/%s", pcs["DefaultEndpointsProtocol"], sa, eps, "%s")
			} else if be := pcs["BlobEndpoint"]; be != "" {
				storageURLFormat = fmt.Sprintf("%s/%s", be, "%s")
			}
		}
	} else {
		storageURLFormat = fmt.Sprintf("https://%s.blob.core.windows.net/%s", storageAccount, "%s")
	}
	if len(storageAccount) == 0 {
		return nil, fmt.Errorf("invalid azure storage credentials")
	}
	if len(storageAccessKey) == 0 {
		credential = azblob.NewAnonymousCredential()
	} else {
		credential, err = azblob.NewSharedKeyCredential(storageAccount, storageAccessKey)
		if err != nil {
			return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
		}
	}
	a := &AzureBlobStorage{
		conf:             conf,
		log:              log,
		stats:            stats,
		timeout:          timeout,
		credential:       credential,
		storageURLFormat: storageURLFormat,
	}
	if a.container, err = bloblang.NewField(conf.Container); err != nil {
		return nil, fmt.Errorf("failed to parse container expression: %v", err)
	}
	if a.path, err = bloblang.NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if a.blobType, err = bloblang.NewField(conf.BlobType); err != nil {
		return nil, fmt.Errorf("failed to parse blob type expression: %v", err)
	}
	return a, nil
}

func extractValues(str string) map[string]string {
	var re = regexp.MustCompile(`(?m)\w+(?:[^=])=(?:[^;])+`)
	values := make(map[string]string)
	for _, match := range re.FindAllString(str, -1) {
		kv := strings.SplitN(match, "=", 2)
		values[kv[0]] = kv[1]
	}
	return values
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

func (a *AzureBlobStorage) getContainer(name string) (*azblob.ContainerURL, error) {
	p := azblob.NewPipeline(a.credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf(a.storageURLFormat, name))
	containerURL := azblob.NewContainerURL(*URL, p)
	return &containerURL, nil
}

func (a *AzureBlobStorage) uploadToBlob(ctx context.Context, message []byte, blobName string, blobType string, containerURL *azblob.ContainerURL) error {
	var err error

	switch blobType {
	case "BLOCK":
		blobURL := containerURL.NewBlockBlobURL(blobName)
		_, err = azblob.UploadStreamToBlockBlob(ctx, bytes.NewReader(message), blobURL, azblob.UploadStreamToBlockBlobOptions{})
	case "APPEND":
		blobURL := containerURL.NewAppendBlobURL(blobName)
		_, err = blobURL.AppendBlock(ctx, bytes.NewReader(message), azblob.AppendBlobAccessConditions{}, nil)
	}

	return err
}

// WriteWithContext attempts to write message contents to a target storage account as files.
func (a *AzureBlobStorage) WriteWithContext(wctx context.Context, msg types.Message) error {
	ctx, cancel := context.WithTimeout(wctx, a.timeout)
	defer cancel()

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		c, err := a.getContainer(a.container.String(i, msg))
		if err != nil {
			return err
		}
		if err := a.uploadToBlob(ctx, p.Get(), a.path.String(i, msg), a.blobType.String(i, msg), c); err != nil {
			if containerNotFound(err) {
				if _, cerr := c.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone); cerr != nil {
					a.log.Errorf("error creating container: %v.", cerr)
				} else {
					a.log.Infof("created container: %s.", c.String())
					// Retry upload to blob
					err = a.uploadToBlob(ctx, p.Get(), a.path.String(i, msg), a.blobType.String(i, msg), c)
				}
			}
			return err
		}
		return nil
	})
}

func containerNotFound(err error) bool {
	if serr, ok := err.(azblob.StorageError); ok {
		return serr.ServiceCode() == azblob.ServiceCodeContainerNotFound
	}
	// TODO azure blob storage api (preview) is returning an internal wrapped storageError
	return strings.Contains(err.Error(), "ContainerNotFound")
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
