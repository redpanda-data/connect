//go:build !wasm
// +build !wasm

package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type azureObjectTarget struct {
	key   string
	ackFn func(context.Context, error) error
}

func newAzureObjectTarget(key string, ackFn codec.ReaderAckFn) *azureObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &azureObjectTarget{key: key, ackFn: ackFn}
}

//------------------------------------------------------------------------------

func deleteAzureObjectAckFn(
	container *storage.Container,
	key string,
	del bool,
	prev codec.ReaderAckFn,
) codec.ReaderAckFn {
	return func(ctx context.Context, err error) error {
		if prev != nil {
			if aerr := prev(ctx, err); aerr != nil {
				return aerr
			}
		}
		if !del || err != nil {
			return nil
		}
		blobReference := container.GetBlobReference(key)
		_, aerr := blobReference.DeleteIfExists(nil)
		return aerr
	}
}

//------------------------------------------------------------------------------

type azurePendingObject struct {
	target    *azureObjectTarget
	obj       *storage.Blob
	extracted int
	scanner   codec.Reader
}

type azureTargetReader struct {
	pending    []*azureObjectTarget
	container  *storage.Container
	conf       AzureBlobStorageConfig
	startAfter string
}

func newAzureTargetReader(
	ctx context.Context,
	conf AzureBlobStorageConfig,
	log log.Modular,
	container *storage.Container,
) (*azureTargetReader, error) {
	params := storage.ListBlobsParameters{
		MaxResults: 100,
	}
	if len(conf.Prefix) > 0 {
		params.Prefix = conf.Prefix
	}
	output, err := container.ListBlobs(params)
	if err != nil {
		return nil, fmt.Errorf("failed to list blobs: %w", err)
	}
	staticKeys := azureTargetReader{
		container: container,
		conf:      conf,
	}
	for _, blob := range output.Blobs {
		ackFn := deleteAzureObjectAckFn(container, blob.Name, conf.DeleteObjects, nil)
		staticKeys.pending = append(staticKeys.pending, newAzureObjectTarget(blob.Name, ackFn))
	}

	if len(output.Blobs) > 0 {
		staticKeys.startAfter = output.NextMarker
	}
	return &staticKeys, nil
}

func (s *azureTargetReader) Pop(ctx context.Context) (*azureObjectTarget, error) {
	if len(s.pending) == 0 && s.startAfter != "" {
		s.pending = nil
		params := storage.ListBlobsParameters{
			Marker:     s.startAfter,
			MaxResults: 100,
		}
		if len(s.conf.Prefix) > 0 {
			params.Prefix = s.conf.Prefix
		}
		output, err := s.container.ListBlobs(params)
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs: %w", err)
		}
		for _, blob := range output.Blobs {
			ackFn := deleteAzureObjectAckFn(s.container, blob.Name, s.conf.DeleteObjects, nil)
			s.pending = append(s.pending, newAzureObjectTarget(blob.Name, ackFn))
		}

		if len(output.Blobs) > 0 {
			s.startAfter = output.NextMarker
		}
	}
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	obj := s.pending[0]
	s.pending = s.pending[1:]
	return obj, nil
}

func (s azureTargetReader) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

// AzureBlobStorage is a benthos reader.Type implementation that reads messages
// from an Azure Blob Storage container.
type azureBlobStorage struct {
	conf AzureBlobStorageConfig

	objectScannerCtor codec.ReaderConstructor
	keyReader         *azureTargetReader

	objectMut sync.Mutex
	object    *azurePendingObject

	container *storage.Container

	log   log.Modular
	stats metrics.Type
}

// newAzureBlobStorage creates a new Azure Blob Storage input type.
func newAzureBlobStorage(conf AzureBlobStorageConfig, log log.Modular, stats metrics.Type) (*azureBlobStorage, error) {
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
			return nil, fmt.Errorf("invalid azure storage SAS token: %w", err)
		}
		client = storage.NewAccountSASClient(conf.StorageAccount, token, azure.PublicCloud)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %w", err)
	}

	var objectScannerCtor codec.ReaderConstructor
	if objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, fmt.Errorf("invalid azure storage codec: %w", err)
	}

	blobService := client.GetBlobService()
	a := &azureBlobStorage{
		conf:              conf,
		objectScannerCtor: objectScannerCtor,
		log:               log,
		stats:             stats,
		container:         blobService.GetContainerReference(conf.Container),
	}

	return a, nil
}

// ConnectWithContext attempts to establish a connection to the target Azure
// Blob Storage container.
func (a *azureBlobStorage) ConnectWithContext(ctx context.Context) error {
	var err error
	a.keyReader, err = newAzureTargetReader(ctx, a.conf, a.log, a.container)
	return err
}

func (a *azureBlobStorage) getObjectTarget(ctx context.Context) (*azurePendingObject, error) {
	if a.object != nil {
		return a.object, nil
	}

	target, err := a.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	blobReference := a.container.GetBlobReference(target.key)
	exists, err := blobReference.Exists()
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, fmt.Errorf("failed to get blob reference: %w", err)
	}

	if !exists {
		_ = target.ackFn(ctx, err)
		return nil, errors.New("blob does not exist")
	}

	obj, err := blobReference.Get(nil)
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	object := &azurePendingObject{
		target: target,
		obj:    blobReference,
	}
	if object.scanner, err = a.objectScannerCtor(target.key, obj, target.ackFn); err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

func blobStorageMsgFromParts(p *azurePendingObject, parts []*message.Part) *message.Batch {
	msg := message.QuickBatch(nil)
	msg.Append(parts...)
	_ = msg.Iter(func(_ int, part *message.Part) error {
		part.MetaSet("blob_storage_key", p.target.key)
		if p.obj.Container != nil {
			part.MetaSet("blob_storage_container", p.obj.Container.Name)
		}
		part.MetaSet("blob_storage_last_modified", time.Time(p.obj.Properties.LastModified).Format(time.RFC3339))
		part.MetaSet("blob_storage_last_modified_unix", strconv.FormatInt(time.Time(p.obj.Properties.LastModified).Unix(), 10))
		part.MetaSet("blob_storage_content_type", p.obj.Properties.ContentType)
		part.MetaSet("blob_storage_content_encoding", p.obj.Properties.ContentEncoding)

		for k, v := range p.obj.Metadata {
			part.MetaSet(k, v)
		}
		return nil
	})

	return msg
}

// ReadWithContext attempts to read a new message from the target Azure Blob
// Storage container.
func (a *azureBlobStorage) ReadWithContext(ctx context.Context) (msg *message.Batch, ackFn reader.AsyncAckFn, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = component.ErrTypeClosed
		} else if serr, ok := err.(storage.AzureStorageServiceError); ok && serr.StatusCode == http.StatusForbidden {
			a.log.Warnf("error downloading blob: %v", err)
			err = component.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = component.ErrTimeout
		}
	}()

	var object *azurePendingObject
	if object, err = a.getObjectTarget(ctx); err != nil {
		return
	}

	var parts []*message.Part
	var scnAckFn codec.ReaderAckFn

	for {
		if parts, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			object.extracted++
			break
		}
		a.object = nil
		if err != io.EOF {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			a.log.Warnf("Failed to close blob object scanner cleanly: %v\n", err)
		}
		if object.extracted == 0 {
			a.log.Debugf("Extracted zero messages from key %v\n", object.target.key)
		}
		if object, err = a.getObjectTarget(ctx); err != nil {
			return
		}
	}

	return blobStorageMsgFromParts(object, parts), func(rctx context.Context, res types.Response) error {
		return scnAckFn(rctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *azureBlobStorage) CloseAsync() {
	go func() {
		a.objectMut.Lock()
		if a.object != nil {
			a.object.scanner.Close(context.Background())
			a.object = nil
		}
		a.objectMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *azureBlobStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
