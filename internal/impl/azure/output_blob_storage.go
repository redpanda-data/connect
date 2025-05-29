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
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Blob Storage Output Fields
	bsoFieldContainer         = "container"
	bsoFieldPath              = "path"
	bsoFieldBlobType          = "blob_type"
	bsoFieldPublicAccessLevel = "public_access_level"
)

type bsoConfig struct {
	client            *azblob.Client
	Container         *service.InterpolatedString
	Path              *service.InterpolatedString
	BlobType          *service.InterpolatedString
	PublicAccessLevel *service.InterpolatedString
}

func bsoConfigFromParsed(pConf *service.ParsedConfig) (conf bsoConfig, err error) {
	if conf.Container, err = pConf.FieldInterpolatedString(bsoFieldContainer); err != nil {
		return
	}
	var containerSASToken bool
	if conf.client, containerSASToken, err = blobStorageClientFromParsed(pConf, conf.Container); err != nil {
		return
	}
	if containerSASToken {
		// if using a container SAS token, the container is already implicit
		conf.Container, _ = service.NewInterpolatedString("")
	}
	if conf.Path, err = pConf.FieldInterpolatedString(bsoFieldPath); err != nil {
		return
	}
	if conf.BlobType, err = pConf.FieldInterpolatedString(bsoFieldBlobType); err != nil {
		return
	}
	if conf.PublicAccessLevel, err = pConf.FieldInterpolatedString(bsoFieldPublicAccessLevel); err != nil {
		return
	}
	return
}

func bsoSpec() *service.ConfigSpec {
	return azureComponentSpec().
		Beta().
		Version("3.36.0").
		Summary(`Sends message parts as objects to an Azure Blob Storage Account container. Each object is uploaded with the filename specified with the `+"`container`"+` field.`).
		Description(`
In order to have a different path for each object you should use function
interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here], which are
calculated per message of a batch.

Supports multiple authentication methods but only one of the following is required:

- `+"`storage_connection_string`"+`
- `+"`storage_account` and `storage_access_key`"+`
- `+"`storage_account` and `storage_sas_token`"+`
- `+"`storage_account` to access via https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential[DefaultAzureCredential^]"+`

If multiple are set then the `+"`storage_connection_string`"+` is given priority.

If the `+"`storage_connection_string`"+` does not contain the `+"`AccountName`"+` parameter, please specify it in the
`+"`storage_account`"+` field.`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewInterpolatedStringField(bsoFieldContainer).
				Description("The container for uploading the messages to.").
				Example(`messages-${!timestamp("2006")}`),
			service.NewInterpolatedStringField(bsoFieldPath).
				Description("The path of each message to upload.").
				Example(`${!counter()}-${!timestamp_unix_nano()}.json`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!counter()}-${!timestamp_unix_nano()}.txt`),
			service.NewInterpolatedStringEnumField(bsoFieldBlobType, "BLOCK", "APPEND").
				Description("Block and Append blobs are comprized of blocks, and each blob can support up to 50,000 blocks. The default value is `+\"`BLOCK`\"+`.`").
				Advanced().
				Default("BLOCK"),
			service.NewInterpolatedStringEnumField(bsoFieldPublicAccessLevel, "PRIVATE", "BLOB", "CONTAINER").
				Description(`The container's public access level. The default value is `+"`PRIVATE`"+`.`).
				Advanced().
				Default("PRIVATE"),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	service.MustRegisterOutput("azure_blob_storage", bsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, mif int, err error) {
			var pConf bsoConfig
			if pConf, err = bsoConfigFromParsed(conf); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newAzureBlobStorageWriter(pConf, mgr.Logger()); err != nil {
				return
			}
			return
		})
}

type azureBlobStorageWriter struct {
	conf bsoConfig
	log  *service.Logger
}

func newAzureBlobStorageWriter(conf bsoConfig, log *service.Logger) (*azureBlobStorageWriter, error) {
	a := &azureBlobStorageWriter{
		conf: conf,
		log:  log,
	}
	return a, nil
}

func (*azureBlobStorageWriter) Connect(context.Context) error {
	return nil
}

func (a *azureBlobStorageWriter) uploadBlob(ctx context.Context, containerName, blobName, blobType string, message []byte) error {
	containerClient := a.conf.client.ServiceClient().NewContainerClient(containerName)
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
	_, err := a.conf.client.CreateContainer(ctx, containerName, &opts)
	return err
}

func (a *azureBlobStorageWriter) Write(ctx context.Context, msg *service.Message) error {
	containerName, err := a.conf.Container.TryString(msg)
	if err != nil {
		return fmt.Errorf("container interpolation error: %s", err)
	}

	blobName, err := a.conf.Path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %s", err)
	}

	blobType, err := a.conf.BlobType.TryString(msg)
	if err != nil {
		return fmt.Errorf("blob type interpolation error: %s", err)
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	if err := a.uploadBlob(ctx, containerName, blobName, blobType, mBytes); err != nil {
		if isErrorCode(err, bloberror.ContainerNotFound) {
			var accessLevel string
			if accessLevel, err = a.conf.PublicAccessLevel.TryString(msg); err != nil {
				return fmt.Errorf("access level interpolation error: %s", err)
			}

			if err := a.createContainer(ctx, containerName, accessLevel); err != nil {
				if !isErrorCode(err, bloberror.ContainerAlreadyExists) {
					return fmt.Errorf("failed to create container: %s", err)
				}
			}

			if err := a.uploadBlob(ctx, containerName, blobName, blobType, mBytes); err != nil {
				return fmt.Errorf("error retrying to upload blob: %s", err)
			}
		} else {
			return fmt.Errorf("failed to upload blob: %s", err)
		}
	}
	return nil
}

func (*azureBlobStorageWriter) Close(context.Context) error {
	return nil
}

func isErrorCode(err error, code bloberror.Code) bool {
	var rerr *azcore.ResponseError
	if ok := errors.As(err, &rerr); ok {
		return rerr.ErrorCode == string(code)
	}

	return false
}
