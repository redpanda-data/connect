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

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Blob Storage Input Fields
	bsiFieldContainer     = "container"
	bsiFieldPrefix        = "prefix"
	bsiFieldCodec         = "codec"
	bsiFieldDeleteObjects = "delete_objects"
)

type bsiConfig struct {
	client        *azblob.Client
	Container     string
	Prefix        string
	Codec         string
	DeleteObjects bool
}

func bsiConfigFromParsed(pConf *service.ParsedConfig) (conf bsiConfig, err error) {
	if conf.client, err = blobStorageClientFromParsed(pConf); err != nil {
		return
	}
	if conf.Container, err = pConf.FieldString(bsiFieldContainer); err != nil {
		return
	}
	if conf.Prefix, err = pConf.FieldString(bsiFieldPrefix); err != nil {
		return
	}
	if conf.Codec, err = pConf.FieldString(bsiFieldCodec); err != nil {
		return
	}
	if conf.DeleteObjects, err = pConf.FieldBool(bsiFieldDeleteObjects); err != nil {
		return
	}
	return
}

func bsiSpec() *service.ConfigSpec {
	return azureComponentSpec(true).
		Beta().
		Version("3.36.0").
		Summary(`Downloads objects within an Azure Blob Storage container, optionally filtered by a prefix.`).
		Description(`
Supports multiple authentication methods but only one of the following is required:
- `+"`storage_connection_string`"+`
- `+"`storage_account` and `storage_access_key`"+`
- `+"`storage_account` and `storage_sas_token`"+`
- `+"`storage_account` to access via [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential)"+`

If multiple are set then the `+"`storage_connection_string`"+` is given priority.

If the `+"`storage_connection_string`"+` does not contain the `+"`AccountName`"+` parameter, please specify it in the
`+"`storage_account`"+` field.

## Downloading Large Files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a `+"[`codec`](#codec)"+` can be specified that determines how to break the input into smaller individual messages.

## Metadata

This input adds the following metadata fields to each message:

`+"```"+`
- blob_storage_key
- blob_storage_container
- blob_storage_last_modified
- blob_storage_last_modified_unix
- blob_storage_content_type
- blob_storage_content_encoding
- All user defined metadata
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(bsiFieldContainer).
				Description("The name of the container from which to download blobs."),
			service.NewStringField(bsiFieldPrefix).
				Description("An optional path prefix, if set only objects with the prefix are consumed.").
				Default(""),
			service.NewInternalField(codec.ReaderDocs).Default("all-bytes"),
			service.NewBoolField(bsiFieldDeleteObjects).
				Description("Whether to delete downloaded objects from the blob once they are processed.").
				Advanced().
				Default(false),
		)
}

func init() {
	err := service.RegisterBatchInput("azure_blob_storage", bsiSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			// NOTE: We're using interop to punch an internal implementation up
			// to the public plugin API. The only blocker from using the full
			// public suite is the codec field.
			//
			// Since codecs are likely to get refactored soon I figured it
			// wasn't worth investing in a public wrapper since the old style
			// will likely get deprecated.
			//
			// This does mean that for now all codec based components will need
			// to keep internal implementations. However, the config specs are
			// the biggest time sink when converting to the new APIs so it's not
			// a big deal to leave these tasks pending.
			conf, err := bsiConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			var rdr input.Async
			if rdr, err = newAzureBlobStorage(conf, res.Logger()); err != nil {
				return nil, err
			}

			rdr = input.NewAsyncPreserver(rdr)
			i, err := input.NewAsyncReader("azure_blob_storage", rdr, mgr)
			if err != nil {
				return nil, err
			}

			return interop.NewUnwrapInternalInput(i), nil
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
	conf       bsiConfig
	startAfter string
}

func newAzureTargetReader(ctx context.Context, conf bsiConfig) (*azureTargetReader, error) {
	var maxResults int32 = 100
	params := &azblob.ListBlobsFlatOptions{
		MaxResults: &maxResults,
	}
	if len(conf.Prefix) > 0 {
		params.Prefix = &conf.Prefix
	}
	output := conf.client.NewListBlobsFlatPager(conf.Container, params)
	staticKeys := azureTargetReader{conf: conf}
	for output.More() {
		page, err := output.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting page of blobs: %w", err)
		}
		for _, blob := range page.Segment.BlobItems {
			ackFn := deleteAzureObjectAckFn(ctx, conf.client, conf.Container, *blob.Name, conf.DeleteObjects, nil)
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
		output := s.conf.client.NewListBlobsFlatPager(s.conf.Container, params)
		for output.More() {
			page, err := output.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("error getting page of blobs: %w", err)
			}
			for _, blob := range page.Segment.BlobItems {
				ackFn := deleteAzureObjectAckFn(ctx, s.conf.client, s.conf.Container, *blob.Name, s.conf.DeleteObjects, nil)
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

type azureBlobStorage struct {
	conf bsiConfig

	objectScannerCtor codec.ReaderConstructor
	keyReader         *azureTargetReader

	objectMut sync.Mutex
	object    *azurePendingObject

	log *service.Logger
}

func newAzureBlobStorage(conf bsiConfig, log *service.Logger) (*azureBlobStorage, error) {
	objectScannerCtor, err := codec.GetReader(conf.Codec, codec.NewReaderConfig())
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage codec: %w", err)
	}
	a := &azureBlobStorage{
		conf:              conf,
		objectScannerCtor: objectScannerCtor,
		log:               log,
	}

	return a, nil
}

func (a *azureBlobStorage) Connect(ctx context.Context) error {
	var err error
	a.keyReader, err = newAzureTargetReader(ctx, a.conf)
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
	obj, err := a.conf.client.DownloadStream(ctx, a.conf.Container, target.key, nil)
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
