package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest/azure"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
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
Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

In order to have a different path for each object you should use function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.`),
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
		).ChildDefaultAndTypesFromStruct(output.NewAzureBlobStorageConfig()),
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
	client      storage.BlobStorageClient
	log         log.Modular
}

func newAzureBlobStorageWriter(mgr bundle.NewManagement, conf output.AzureBlobStorageConfig, log log.Modular) (*azureBlobStorageWriter, error) {
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
	a := &azureBlobStorageWriter{
		conf:   conf,
		log:    log,
		client: client.GetBlobService(),
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

func (a *azureBlobStorageWriter) uploadBlob(b *storage.Blob, blobType string, message []byte) error {
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

func (a *azureBlobStorageWriter) createContainer(c *storage.Container, accessLevel string) error {
	opts := storage.CreateContainerOptions{}
	switch accessLevel {
	case "BLOB":
		opts.Access = storage.ContainerAccessTypeBlob
	case "CONTAINER":
		opts.Access = storage.ContainerAccessTypeContainer
	}
	return c.Create(&opts)
}

func (a *azureBlobStorageWriter) WriteBatch(_ context.Context, msg message.Batch) error {
	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		containerStr, err := a.container.String(i, msg)
		if err != nil {
			return fmt.Errorf("container interpolation error: %w", err)
		}

		pathStr, err := a.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}

		blobTypeStr, err := a.blobType.String(i, msg)
		if err != nil {
			return fmt.Errorf("blob type interpolation error: %w", err)
		}

		c := a.client.GetContainerReference(containerStr)
		b := c.GetBlobReference(pathStr)
		if err = a.uploadBlob(b, blobTypeStr, p.AsBytes()); err != nil {
			if containerNotFound(err) {
				var accessLevelStr string
				if accessLevelStr, err = a.accessLevel.String(i, msg); err != nil {
					return fmt.Errorf("access level interpolation error: %w", err)
				}

				if cerr := a.createContainer(c, accessLevelStr); cerr != nil {
					a.log.Debugf("error creating container: %v.", cerr)
					return cerr
				}

				if err = a.uploadBlob(b, blobTypeStr, p.AsBytes()); err != nil {
					a.log.Debugf("error retrying to upload blob: %v.", err)
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

func (a *azureBlobStorageWriter) Close(context.Context) error {
	return nil
}
