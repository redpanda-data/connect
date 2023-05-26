package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
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
		r, err := newAzureBlobStorage(conf.AzureBlobStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("azure_blob_storage", input.NewAsyncPreserver(r), nm)
	}), docs.ComponentSpec{
		Name:    "azure_blob_storage",
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

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"storage_account",
				"The storage account to download blobs from. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_sas_token",
				"The storage account SAS token. This field is ignored if `storage_connection_string` or `storage_access_key` are set.",
			).AtVersion("3.38.0"),
			docs.FieldString(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` / `storage_sas_token` are not set.",
			),
			docs.FieldString(
				"container", "The name of the container from which to download blobs.",
			),
			docs.FieldString("prefix", "An optional path prefix, if set only objects with the prefix are consumed."),
			codec.ReaderDocs,
			docs.FieldBool("delete_objects", "Whether to delete downloaded objects from the blob once they are processed.").Advanced(),
		).LinterBlobl(
			`root = if this.storage_connection_string != "" && !this.storage_connection_string.contains("AccountName=") && this.storage_account == "" { [ "storage_account must be set if storage_connection_string does not contain the \"AccountName\" parameter" ] }`,
		).ChildDefaultAndTypesFromStruct(input.NewAzureBlobStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
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
	ctx context.Context,
	client *azblob.Client,
	containerName string,
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
		_, err = client.DeleteBlob(ctx, containerName, key, nil)
		return err
	}
}

//------------------------------------------------------------------------------

type azurePendingObject struct {
	target    *azureObjectTarget
	obj       azblob.DownloadStreamResponse
	extracted int
	scanner   codec.Reader
}

type azureTargetReader struct {
	pending    []*azureObjectTarget
	client     *azblob.Client
	conf       input.AzureBlobStorageConfig
	startAfter string
}

func newAzureTargetReader(
	ctx context.Context,
	conf input.AzureBlobStorageConfig,
	log log.Modular,
	client *azblob.Client,
) (*azureTargetReader, error) {
	var maxResults int32 = 100
	params := &azblob.ListBlobsFlatOptions{
		MaxResults: &maxResults,
	}
	if len(conf.Prefix) > 0 {
		params.Prefix = &conf.Prefix
	}
	output := client.NewListBlobsFlatPager(conf.Container, params)
	staticKeys := azureTargetReader{
		client: client,
		conf:   conf,
	}
	for output.More() {
		page, err := output.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting page of blobs: %w", err)
		}
		for _, blob := range page.Segment.BlobItems {
			ackFn := deleteAzureObjectAckFn(ctx, client, conf.Container, *blob.Name, conf.DeleteObjects, nil)
			staticKeys.pending = append(staticKeys.pending, newAzureObjectTarget(*blob.Name, ackFn))
		}
		staticKeys.startAfter = *page.NextMarker
	}

	return &staticKeys, nil
}

func (s *azureTargetReader) Pop(ctx context.Context) (*azureObjectTarget, error) {
	if len(s.pending) == 0 && s.startAfter != "" {
		s.pending = nil
		var maxResults int32 = 100
		params := &azblob.ListBlobsFlatOptions{
			MaxResults: &maxResults,
			Marker:     &s.startAfter,
		}
		if len(s.conf.Prefix) > 0 {
			params.Prefix = &s.conf.Prefix
		}
		output := s.client.NewListBlobsFlatPager(s.conf.Container, params)
		for output.More() {
			page, err := output.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting page of blobs: %w", err)
			}
			for _, blob := range page.Segment.BlobItems {
				ackFn := deleteAzureObjectAckFn(ctx, s.client, s.conf.Container, *blob.Name, s.conf.DeleteObjects, nil)
				s.pending = append(s.pending, newAzureObjectTarget(*blob.Name, ackFn))
			}
			s.startAfter = *page.NextMarker
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
	conf input.AzureBlobStorageConfig

	objectScannerCtor codec.ReaderConstructor
	keyReader         *azureTargetReader

	objectMut sync.Mutex
	object    *azurePendingObject

	client *azblob.Client

	log   log.Modular
	stats metrics.Type
}

// newAzureBlobStorage creates a new Azure Blob Storage input type.
func newAzureBlobStorage(conf input.AzureBlobStorageConfig, log log.Modular, stats metrics.Type) (*azureBlobStorage, error) {
	if conf.StorageAccount == "" && conf.StorageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}

	client, err := shared.GetStorageClient(conf.StorageConnectionString, conf.StorageAccount, conf.StorageAccessKey, conf.StorageSASToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage client: %v", err)
	}

	var objectScannerCtor codec.ReaderConstructor
	if objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, fmt.Errorf("invalid azure storage codec: %w", err)
	}
	a := &azureBlobStorage{
		conf:              conf,
		objectScannerCtor: objectScannerCtor,
		log:               log,
		stats:             stats,
		client:            client,
	}

	return a, nil
}

// Connect attempts to establish a connection to the target Azure
// Blob Storage container.
func (a *azureBlobStorage) Connect(ctx context.Context) error {
	var err error
	a.keyReader, err = newAzureTargetReader(ctx, a.conf, a.log, a.client)
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
	obj, err := a.client.DownloadStream(ctx, a.conf.Container, target.key, nil)
	if err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	object := &azurePendingObject{
		target: target,
		obj:    obj,
	}
	if object.scanner, err = a.objectScannerCtor(target.key, obj.NewRetryReader(ctx, nil), target.ackFn); err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

func blobStorageMsgFromParts(p *azurePendingObject, containerName string, parts []*message.Part) message.Batch {
	msg := message.Batch(parts)
	_ = msg.Iter(func(_ int, part *message.Part) error {
		part.MetaSetMut("blob_storage_key", p.target.key)
		part.MetaSetMut("blob_storage_container", containerName)
		part.MetaSetMut("blob_storage_last_modified", p.obj.LastModified.Format(time.RFC3339))
		part.MetaSetMut("blob_storage_last_modified_unix", p.obj.LastModified.Unix())
		part.MetaSetMut("blob_storage_content_type", p.obj.ContentType)
		part.MetaSetMut("blob_storage_content_encoding", p.obj.ContentEncoding)

		for k, v := range p.obj.Metadata {
			part.MetaSetMut(k, v)
		}
		return nil
	})
	return msg
}

// ReadBatch attempts to read a new message from the target Azure Blob
// Storage container.
func (a *azureBlobStorage) ReadBatch(ctx context.Context) (msg message.Batch, ackFn input.AsyncAckFn, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = component.ErrTypeClosed
		} else if serr, ok := err.(*azcore.ResponseError); ok && serr.StatusCode == http.StatusForbidden {
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

	return blobStorageMsgFromParts(object, a.conf.Container, parts), func(rctx context.Context, res error) error {
		return scnAckFn(rctx, res)
	}, nil
}

func (a *azureBlobStorage) Close(ctx context.Context) (err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	if a.object != nil {
		err = a.object.scanner.Close(ctx)
		a.object = nil
	}
	return
}
