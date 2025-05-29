// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Jeffail/gabs/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/codec"
)

const (
	// Blob Storage Input Fields
	bsiFieldContainer     = "container"
	bsiFieldPrefix        = "prefix"
	bsiFieldDeleteObjects = "delete_objects"
	bsiFieldTargetsInput  = "targets_input"
)

type bsiConfig struct {
	client        *azblob.Client
	Container     string
	Prefix        string
	DeleteObjects bool
	FileReader    *service.OwnedInput
	Codec         codec.DeprecatedFallbackCodec
}

func bsiConfigFromParsed(pConf *service.ParsedConfig) (conf bsiConfig, err error) {
	var containerSASToken bool
	container, err := pConf.FieldInterpolatedString(bsiFieldContainer)
	if err != nil {
		return
	}
	if conf.client, containerSASToken, err = blobStorageClientFromParsed(pConf, container); err != nil {
		return
	}
	if containerSASToken {
		// if using a container SAS token, the container is already implicit
		container, _ = service.NewInterpolatedString("")
	}
	if conf.Container, err = container.TryString(service.NewMessage([]byte(""))); err != nil {
		return
	}
	if conf.Prefix, err = pConf.FieldString(bsiFieldPrefix); err != nil {
		return
	}
	if conf.Codec, err = codec.DeprecatedCodecFromParsed(pConf); err != nil {
		return
	}
	if conf.DeleteObjects, err = pConf.FieldBool(bsiFieldDeleteObjects); err != nil {
		return
	}
	if pConf.Contains(bsiFieldTargetsInput) {
		if conf.FileReader, err = pConf.FieldInput(bsiFieldTargetsInput); err != nil {
			return
		}
	}
	return
}

func bsiSpec() *service.ConfigSpec {
	return azureComponentSpec().
		Beta().
		Version("3.36.0").
		Summary(`Downloads objects within an Azure Blob Storage container, optionally filtered by a prefix.`).
		Description(`
Supports multiple authentication methods but only one of the following is required:

- `+"`storage_connection_string`"+`
- `+"`storage_account` and `storage_access_key`"+`
- `+"`storage_account` and `storage_sas_token`"+`
- `+"`storage_account` to access via https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential[DefaultAzureCredential^]"+`

If multiple are set then the `+"`storage_connection_string`"+` is given priority.

If the `+"`storage_connection_string`"+` does not contain the `+"`AccountName`"+` parameter, please specify it in the
`+"`storage_account`"+` field.

== Download large files

When downloading large files it's often necessary to process it in streamed parts in order to avoid loading the entire file in memory at a given time. In order to do this a `+"<<scanner, `scanner`>>"+` can be specified that determines how to break the input into smaller individual messages.

== Stream new files

By default this input will consume all files found within the target container and will then gracefully terminate. This is referred to as a "batch" mode of operation. However, it's possible to instead configure a container as https://learn.microsoft.com/en-gb/azure/event-grid/event-schema-blob-storage[an Event Grid source^] and then use this as a `+"<<targetsinput, `targets_input`>>"+`, in which case new files are consumed as they're uploaded and Redpanda Connect will continue listening for and downloading files as they arrive. This is referred to as a "streamed" mode of operation.

== Metadata

This input adds the following metadata fields to each message:

- blob_storage_key
- blob_storage_container
- blob_storage_last_modified
- blob_storage_last_modified_unix
- blob_storage_content_type
- blob_storage_content_encoding
- All user defined metadata

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewInterpolatedStringField(bsiFieldContainer).
				Description("The name of the container from which to download blobs."),
			service.NewStringField(bsiFieldPrefix).
				Description("An optional path prefix, if set only objects with the prefix are consumed.").
				Default(""),
		).
		Fields(codec.DeprecatedCodecFields("to_the_end")...).
		Fields(
			service.NewBoolField(bsiFieldDeleteObjects).
				Description("Whether to delete downloaded objects from the blob once they are processed.").
				Advanced().
				Default(false),
			service.NewInputField(bsiFieldTargetsInput).
				Description("EXPERIMENTAL: An optional source of download targets, configured as a xref:components:inputs/about.adoc[regular Redpanda Connect input]. Each message yielded by this input should be a single structured object containing a field `name`, which represents the blob to be downloaded.").
				Optional().
				Version("4.27.0").
				Example(map[string]any{
					"mqtt": map[string]any{
						"urls": []any{
							"example.westeurope-1.ts.eventgrid.azure.net:8883",
						},
						"topics": []any{
							"some-topic",
						},
					},
					"processors": []any{
						map[string]any{
							"unarchive": map[string]any{
								"format": "json_array",
							},
						},
						map[string]any{
							"mapping": `if this.eventType == "Microsoft.Storage.BlobCreated" {
  root.name = this.data.url.parse_url().path.trim_prefix("/foocontainer/")
} else {
  root = deleted()
}`,
						},
					},
				}),
		)
}

func init() {
	service.MustRegisterBatchInput("azure_blob_storage", bsiSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			conf, err := bsiConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			var rdr service.BatchInput
			if rdr, err = newAzureBlobStorage(conf, res.Logger()); err != nil {
				return nil, err
			}

			if conf.FileReader == nil {
				rdr = service.AutoRetryNacksBatched(rdr)
			}
			return rdr, nil
		})
}

//------------------------------------------------------------------------------

type azureObjectTarget struct {
	key   string
	ackFn func(context.Context, error) error
}

func newAzureObjectTarget(key string, ackFn service.AckFunc) *azureObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &azureObjectTarget{key: key, ackFn: ackFn}
}

//------------------------------------------------------------------------------

func deleteAzureObjectAckFn(
	client *azblob.Client,
	containerName string,
	key string,
	del bool,
	prev service.AckFunc,
) service.AckFunc {
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
	scanner   codec.DeprecatedFallbackStream
}

type azureTargetReader interface {
	Pop(ctx context.Context) (*azureObjectTarget, error)
	Close(context.Context) error
}

func newAzureTargetReader(ctx context.Context, logger *service.Logger, conf bsiConfig) (azureTargetReader, error) {
	if conf.FileReader == nil {
		return newAzureTargetBatchReader(ctx, conf)
	}
	return &azureTargetStreamReader{
		conf:  conf,
		input: conf.FileReader,
		log:   logger,
	}, nil
}

//------------------------------------------------------------------------------

type azureTargetStreamReader struct {
	pending []*azureObjectTarget
	conf    bsiConfig
	input   *service.OwnedInput
	log     *service.Logger
}

func (a *azureTargetStreamReader) Pop(ctx context.Context) (*azureObjectTarget, error) {
	if len(a.pending) > 0 {
		t := a.pending[0]
		a.pending = a.pending[1:]
		return t, nil
	}

	for {
		next, ackFn, err := a.input.ReadBatch(ctx)
		if err != nil {
			if errors.Is(err, service.ErrEndOfInput) {
				return nil, io.EOF
			}
			return nil, err
		}

		var pendingAcks int32
		var nackOnce sync.Once
		for _, msg := range next {
			mStructured, err := msg.AsStructured()
			if err != nil {
				a.log.With("error", err).Error("Failed to extract structured object from targets input message")
				continue
			}

			name, _ := gabs.Wrap(mStructured).S("name").Data().(string)
			if name == "" {
				a.log.Warn("Targets input yielded a message that did not contain a `name` field")
				continue
			}

			pendingAcks++

			var ackOnce sync.Once
			a.pending = append(a.pending, &azureObjectTarget{
				key: name,
				ackFn: func(ctx context.Context, err error) (aerr error) {
					keyNotFound := false
					var rErr *azcore.ResponseError
					if errors.As(err, &rErr) {
						if rErr.ErrorCode == string(bloberror.BlobNotFound) {
							a.log.Warnf("Skipping missing blob: %s", name)
							keyNotFound = true
						}
					}
					if err != nil && !keyNotFound {
						nackOnce.Do(func() {
							// Prevent future acks from triggering a delete.
							atomic.StoreInt32(&pendingAcks, -1)

							// It's possible that this is called for one message
							// at the _exact_ same time as another is acked, but
							// if the acked message triggers a full ack of the
							// origin message then even though it shouldn't be
							// possible, it's also harmless.
							aerr = ackFn(ctx, err)
						})
					} else {
						ackOnce.Do(func() {
							if atomic.AddInt32(&pendingAcks, -1) == 0 {
								ackFn := deleteAzureObjectAckFn(a.conf.client, a.conf.Container, name, a.conf.DeleteObjects, ackFn)
								aerr = ackFn(ctx, nil)
							}
						})
					}
					return
				},
			})
		}

		if len(a.pending) > 0 {
			t := a.pending[0]
			a.pending = a.pending[1:]
			return t, nil
		} else {
			// Ack the messages even though we didn't extract any valid names.
			_ = ackFn(ctx, nil)
		}
	}
}

func (a *azureTargetStreamReader) Close(ctx context.Context) error {
	for _, p := range a.pending {
		_ = p.ackFn(ctx, errors.New("shutting down"))
	}
	return a.input.Close(ctx)
}

//------------------------------------------------------------------------------

type azureTargetBatchReader struct {
	pending []*azureObjectTarget
	conf    bsiConfig
	pager   *runtime.Pager[azblob.ListBlobsFlatResponse]
}

func newAzureTargetBatchReader(ctx context.Context, conf bsiConfig) (*azureTargetBatchReader, error) {
	var maxResults int32 = 100
	params := &azblob.ListBlobsFlatOptions{
		MaxResults: &maxResults,
	}
	if conf.Prefix != "" {
		params.Prefix = &conf.Prefix
	}
	pager := conf.client.NewListBlobsFlatPager(conf.Container, params)
	staticKeys := azureTargetBatchReader{conf: conf}
	if pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting page of blobs: %w", err)
		}
		for _, blob := range page.Segment.BlobItems {
			ackFn := deleteAzureObjectAckFn(conf.client, conf.Container, *blob.Name, conf.DeleteObjects, nil)
			staticKeys.pending = append(staticKeys.pending, newAzureObjectTarget(*blob.Name, ackFn))
		}
		staticKeys.pager = pager
	}
	return &staticKeys, nil
}

func (s *azureTargetBatchReader) Pop(ctx context.Context) (*azureObjectTarget, error) {
	if len(s.pending) == 0 && s.pager.More() {
		s.pending = nil
		page, err := s.pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting page of blobs: %w", err)
		}
		for _, blob := range page.Segment.BlobItems {
			ackFn := deleteAzureObjectAckFn(s.conf.client, s.conf.Container, *blob.Name, s.conf.DeleteObjects, nil)
			s.pending = append(s.pending, newAzureObjectTarget(*blob.Name, ackFn))
		}
	}
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	obj := s.pending[0]
	s.pending = s.pending[1:]
	return obj, nil
}

func (azureTargetBatchReader) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type azureBlobStorage struct {
	conf bsiConfig

	objectScannerCtor codec.DeprecatedFallbackCodec
	keyReader         azureTargetReader

	objectMut sync.Mutex
	object    *azurePendingObject

	log *service.Logger
}

func newAzureBlobStorage(conf bsiConfig, log *service.Logger) (*azureBlobStorage, error) {
	a := &azureBlobStorage{
		conf:              conf,
		objectScannerCtor: conf.Codec,
		log:               log,
	}
	return a, nil
}

func (a *azureBlobStorage) Connect(ctx context.Context) error {
	var err error
	a.keyReader, err = newAzureTargetReader(ctx, a.log, a.conf)
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
	details := service.NewScannerSourceDetails()
	details.SetName(target.key)
	if object.scanner, err = a.objectScannerCtor.Create(obj.NewRetryReader(ctx, nil), target.ackFn, details); err != nil {
		_ = target.ackFn(ctx, err)
		return nil, err
	}

	a.object = object
	return object, nil
}

func blobStorageMetaToBatch(p *azurePendingObject, containerName string, parts service.MessageBatch) {
	for _, part := range parts {
		part.MetaSetMut("blob_storage_key", p.target.key)
		part.MetaSetMut("blob_storage_container", containerName)
		if p.obj.LastModified != nil {
			part.MetaSetMut("blob_storage_last_modified", p.obj.LastModified.Format(time.RFC3339))
			part.MetaSetMut("blob_storage_last_modified_unix", p.obj.LastModified.Unix())
		}
		if p.obj.ContentType != nil {
			part.MetaSetMut("blob_storage_content_type", *p.obj.ContentType)
		}
		if p.obj.ContentEncoding != nil {
			part.MetaSetMut("blob_storage_content_encoding", *p.obj.ContentEncoding)
		}

		for k, v := range p.obj.Metadata {
			part.MetaSetMut(k, v)
		}
	}
}

func (a *azureBlobStorage) ReadBatch(ctx context.Context) (msg service.MessageBatch, ackFn service.AckFunc, err error) {
	a.objectMut.Lock()
	defer a.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = service.ErrEndOfInput
		} else if serr, ok := err.(*azcore.ResponseError); ok && serr.StatusCode == http.StatusForbidden {
			a.log.Warnf("error downloading blob: %v", err)
			err = service.ErrEndOfInput
		}
	}()

	var object *azurePendingObject
	if object, err = a.getObjectTarget(ctx); err != nil {
		return
	}

	var parts service.MessageBatch
	var scnAckFn service.AckFunc

	for {
		if parts, scnAckFn, err = object.scanner.NextBatch(ctx); err == nil {
			object.extracted++
			break
		}
		a.object = nil
		if err != io.EOF {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			a.log.Warnf("Failed to close blob object scanner cleanly: %v", err)
		}
		if object.extracted == 0 {
			a.log.Debugf("Extracted zero messages from key %v", object.target.key)
		}
		if object, err = a.getObjectTarget(ctx); err != nil {
			return
		}
	}

	blobStorageMetaToBatch(object, a.conf.Container, parts)

	return parts, func(rctx context.Context, res error) error {
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
