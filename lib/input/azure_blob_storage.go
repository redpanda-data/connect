package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	Constructors[TypeAzureBlobStorage] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newAzureBlobStorage(conf.AzureBlobStorage, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeAzureBlobStorage,
				true,
				reader.NewAsyncBundleUnacks(
					reader.NewAsyncPreserver(r),
				),
				log, stats,
			)
		},
		Status:  docs.StatusBeta,
		Version: "3.36.0",
		Summary: `
Downloads objects within an Azure Blob Storage container, optionally filtered by
a prefix.`,
		Description: `
Downloads objects within an Azure Blob Storage container, optionally filtered by a prefix.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a ` + "[`codec`](#codec)" + ` can be specified that determines how to break the input into smaller individual messages.

## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- blob_storage_key
- blob_storage_container
- blob_storage_last_modified
- blob_storage_last_modified_unix
- blob_storage_content_type
- blob_storage_content_encoding
- All user defined metadata
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"storage_account",
				"The storage account to download blobs from. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldCommon(
				"container", "The name of the container from which to download blobs.",
			),
			docs.FieldCommon("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the blob once they are processed."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage
// input type.
type AzureBlobStorageConfig struct {
	StorageAccount          string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageConnectionString string `json:"storage_connection_string" yaml:"storage_connection_string"`
	Container               string `json:"container" yaml:"container"`
	Prefix                  string `json:"prefix" yaml:"prefix"`
	Codec                   string `json:"codec" yaml:"codec"`
	DeleteObjects           bool   `json:"delete_objects" yaml:"delete_objects"`
}

// NewAzureBlobStorageConfig creates a new AzureBlobStorageConfig with default
// values.
func NewAzureBlobStorageConfig() AzureBlobStorageConfig {
	return AzureBlobStorageConfig{
		Codec: "all-bytes",
	}
}

//------------------------------------------------------------------------------

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
	delete bool,
	prev codec.ReaderAckFn,
) codec.ReaderAckFn {
	return func(ctx context.Context, err error) error {
		if prev != nil {
			if aerr := prev(ctx, err); aerr != nil {
				return aerr
			}
		}
		if !delete || err != nil {
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
		return nil, fmt.Errorf("failed to list blobs: %v", err)
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
			return nil, fmt.Errorf("failed to list blobs: %v", err)
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
	if len(conf.StorageAccount) == 0 && len(conf.StorageConnectionString) == 0 {
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
	} else {
		client, err = storage.NewBasicClient(conf.StorageAccount, conf.StorageAccessKey)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}

	var objectScannerCtor codec.ReaderConstructor
	if objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, fmt.Errorf("invalid azure storage codec: %v", err)
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
		target.ackFn(ctx, err)
		return nil, fmt.Errorf("failed to get blob reference: %v", err)
	}

	if !exists {
		target.ackFn(ctx, err)
		return nil, errors.New("blob does not exist")
	}

	obj, err := blobReference.Get(nil)
	if err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	object := &azurePendingObject{
		target: target,
		obj:    blobReference,
	}
	if object.scanner, err = a.objectScannerCtor(target.key, obj, target.ackFn); err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

func blobStorageMsgFromPart(p *azurePendingObject, part types.Part) types.Message {
	msg := message.New(nil)
	msg.Append(part)

	meta := msg.Get(0).Metadata()

	meta.Set("blob_storage_key", p.target.key)
	if p.obj.Container != nil {
		meta.Set("blob_storage_container", p.obj.Container.Name)
	}
	meta.Set("blob_storage_last_modified", time.Time(p.obj.Properties.LastModified).Format(time.RFC3339))
	meta.Set("blob_storage_last_modified_unix", strconv.FormatInt(time.Time(p.obj.Properties.LastModified).Unix(), 10))
	meta.Set("blob_storage_content_type", p.obj.Properties.ContentType)
	meta.Set("blob_storage_content_encoding", p.obj.Properties.ContentEncoding)

	for k, v := range p.obj.Metadata {
		meta.Set(k, v)
	}

	return msg
}

// ReadWithContext attempts to read a new message from the target Azure Blob
// Storage container.
func (a *azureBlobStorage) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = types.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = types.ErrTimeout
		}
	}()

	var object *azurePendingObject
	if object, err = a.getObjectTarget(ctx); err != nil {
		return
	}

	var p types.Part
	var scnAckFn codec.ReaderAckFn

scanLoop:
	for {
		if p, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			object.extracted++
			break scanLoop
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

	return blobStorageMsgFromPart(object, p), func(rctx context.Context, res types.Response) error {
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
