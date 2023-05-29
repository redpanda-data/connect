package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newAzureBlobStorageOutput(conf, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "azure_blob_storage",
		Status:  docs.StatusBeta,
		Version: "3.36.0",
		Summary: `
Sends message parts as objects to an Azure Blob Storage Account container. Each
object is uploaded with the filename specified with the ` + "`container`" + `
field.`,
		Description: output.Description(true, false, `
In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

Supports multiple authentication methods but only one of the following is required:
- `+"`storage_connection_string`"+`
- `+"`storage_account` and `storage_access_key`"+`
- `+"`storage_account` and `storage_sas_token`"+`
- `+"`storage_account` to access via [DefaultAzureCredential](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential)"+`

If multiple are set then the `+"`storage_connection_string`"+` is given priority.

If the `+"`storage_connection_string`"+` does not contain the `+"`AccountName`"+` parameter, please specify it in the
`+"`storage_account`"+` field.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"storage_account",
				"The storage account to upload messages to. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_sas_token",
				"The storage account SAS token. This field is ignored if `storage_connection_string` or `storage_access_key` / `storage_sas_token` are set.",
			).AtVersion("3.38.0"),
			docs.FieldString(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldString("public_access_level", `The container's public access level. The default value is `+"`PRIVATE`"+`.`).HasOptions(
				"PRIVATE", "BLOB", "CONTAINER",
			).Advanced(),
			docs.FieldString(
				"container", "The container for uploading the messages to.",
				`messages-${!timestamp("2006")}`,
			).IsInterpolated(),
			docs.FieldString(
				"path", "The path of each message to upload.",
				`${!count("files")}-${!timestamp_unix_nano()}.json`,
				`${!meta("kafka_key")}.json`,
				`${!json("doc.namespace")}/${!json("doc.id")}.json`,
			).IsInterpolated(),
			docs.FieldString("blob_type", "Block and Append blobs are comprised of blocks, and each blob can support up to 50,000 blocks. The default value is `+\"`BLOCK`\"+`.`").HasOptions(
				"BLOCK", "APPEND",
			).IsInterpolated().Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).LinterBlobl(
			`root = if this.storage_connection_string != "" && !this.storage_connection_string.contains("AccountName=") && this.storage_account == "" { [ "storage_account must be set if storage_connection_string does not contain the \"AccountName\" parameter" ] }`).
			ChildDefaultAndTypesFromStruct(output.NewAzureBlobStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newAzureBlobStorageOutput(conf output.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	blobStorage, err := newAzureBlobStorageWriter(mgr, conf.AzureBlobStorage, log)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("azure_blob_storage", conf.AzureBlobStorage.MaxInFlight, blobStorage, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type azureBlobStorageWriter struct {
	conf        output.AzureBlobStorageConfig
	container   *field.Expression
	path        *field.Expression
	blobType    *field.Expression
	accessLevel *field.Expression
	client      *azblob.Client
	log         log.Modular
}

func newAzureBlobStorageWriter(mgr bundle.NewManagement, conf output.AzureBlobStorageConfig, log log.Modular) (*azureBlobStorageWriter, error) {
	if conf.StorageAccount == "" && conf.StorageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}

	client, err := shared.GetBlobStorageClient(conf.StorageConnectionString, conf.StorageAccount, conf.StorageAccessKey, conf.StorageSASToken)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage client: %v", err)
	}

	a := &azureBlobStorageWriter{
		conf:   conf,
		log:    log,
		client: client,
	}
	if a.container, err = mgr.BloblEnvironment().NewField(conf.Container); err != nil {
		return nil, fmt.Errorf("failed to parse container expression: %v", err)
	}
	if a.path, err = mgr.BloblEnvironment().NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	if a.blobType, err = mgr.BloblEnvironment().NewField(conf.BlobType); err != nil {
		return nil, fmt.Errorf("failed to parse blob type expression: %v", err)
	}
	if a.accessLevel, err = mgr.BloblEnvironment().NewField(conf.PublicAccessLevel); err != nil {
		return nil, fmt.Errorf("failed to parse public access level expression: %v", err)
	}
	return a, nil
}

func (a *azureBlobStorageWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureBlobStorageWriter) uploadBlob(ctx context.Context, containerName, blobName, blobType string, message []byte) error {
	containerClient := a.client.ServiceClient().NewContainerClient(containerName)
	var err error
	if blobType == "APPEND" {
		appendBlobClient := containerClient.NewAppendBlobClient(blobName)
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(message)), nil)
		if err != nil {
			if isErrorCode(err, bloberror.BlobNotFound) {
				_, err := appendBlobClient.Create(ctx, nil)
				if err != nil && !isErrorCode(err, bloberror.BlobAlreadyExists) {
					return fmt.Errorf("failed to create append blob: %w", err)
				}

				// Try to upload the message again now that we created the blob
				_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(message)), nil)
				if err != nil {
					return fmt.Errorf("failed retrying to append block to blob: %w", err)
				}
			} else {
				return fmt.Errorf("failed to append block to blob: %w", err)
			}
		}
	} else {
		_, err = containerClient.NewBlockBlobClient(blobName).UploadStream(ctx, bytes.NewReader(message), nil)
		if err != nil {
			return fmt.Errorf("failed to push block to blob: %w", err)
		}
	}
	return nil
}

func (a *azureBlobStorageWriter) createContainer(ctx context.Context, containerName, accessLevel string) error {
	var opts azblob.CreateContainerOptions
	switch accessLevel {
	case "BLOB":
		accessType := azblob.PublicAccessTypeBlob
		opts.Access = &accessType
	case "CONTAINER":
		accessType := azblob.PublicAccessTypeContainer
		opts.Access = &accessType
	}
	_, err := a.client.CreateContainer(ctx, containerName, &opts)
	return err
}

func (a *azureBlobStorageWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		containerName, err := a.container.String(i, msg)
		if err != nil {
			return fmt.Errorf("container interpolation error: %s", err)
		}

		blobName, err := a.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %s", err)
		}

		blobType, err := a.blobType.String(i, msg)
		if err != nil {
			return fmt.Errorf("blob type interpolation error: %s", err)
		}

		if err := a.uploadBlob(ctx, containerName, blobName, blobType, p.AsBytes()); err != nil {
			if isErrorCode(err, bloberror.ContainerNotFound) {
				var accessLevel string
				if accessLevel, err = a.accessLevel.String(i, msg); err != nil {
					return fmt.Errorf("access level interpolation error: %s", err)
				}

				if err := a.createContainer(ctx, containerName, accessLevel); err != nil {
					if !isErrorCode(err, bloberror.ContainerAlreadyExists) {
						return fmt.Errorf("failed to create container: %s", err)
					}
				}

				if err := a.uploadBlob(ctx, containerName, blobName, blobType, p.AsBytes()); err != nil {
					return fmt.Errorf("error retrying to upload blob: %s", err)
				}
			} else {
				return fmt.Errorf("failed to upload blob: %s", err)
			}
		}
		return nil
	})
}

func (a *azureBlobStorageWriter) Close(context.Context) error {
	return nil
}

func isErrorCode(err error, code bloberror.Code) bool {
	var rerr *azcore.ResponseError
	if ok := errors.As(err, &rerr); ok {
		return rerr.ErrorCode == string(code)
	}

	return false
}
