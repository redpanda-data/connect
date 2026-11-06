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
	"maps"
	"slices"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Blob Storage Output Fields
	bsoFieldContainer         = "container"
	bsoFieldPath              = "path"
	bsoFieldTags              = "tags"
	bsoFieldBlobType          = "blob_type"
	bsoFieldPublicAccessLevel = "public_access_level"

	// The following blob index tag limits are imposed by Azure. See
	// https://learn.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs

	// bsoMaxTagsPermitted is the maximum number of blob index tags a single
	// blob may carry.
	bsoMaxTagsPermitted = 10
	// bsoMaxTagKeyLength is the maximum length of a blob index tag key.
	bsoMaxTagKeyLength = 128
	// bsoMaxTagValueLength is the maximum length of a blob index tag value.
	bsoMaxTagValueLength = 256
	// bsoTagAllowedSpecialChars are the non-alphanumeric characters permitted
	// within blob index tag keys and values.
	bsoTagAllowedSpecialChars = " +-./:=_"
)

// bsoValidateTagKey checks a blob index tag key against the length and character
// set limits imposed by Azure. Tag keys are always static, so an invalid one is
// worth rejecting at config time rather than failing every message. Tag values
// are interpolated per message and so cannot be checked here.
func bsoValidateTagKey(key string) error {
	if r, ok := bsoFirstInvalidTagChar(key); ok {
		return fmt.Errorf("blob index tag key %q contains unsupported character %q, only alphanumerics and the characters %q are permitted", key, r, bsoTagAllowedSpecialChars)
	}
	// Only ASCII characters reach here, so byte length matches the character
	// count Azure counts against the limit.
	if l := len(key); l < 1 || l > bsoMaxTagKeyLength {
		return fmt.Errorf("blob index tag key %q must be between 1 and %d characters, got %d", key, bsoMaxTagKeyLength, l)
	}
	return nil
}

// bsoValidateTagValue checks an interpolated blob index tag value against the
// length and character set limits imposed by Azure. Azure rejects the whole
// upload for a bad value, so checking first gives a clearer error. The error
// names the key but not the value, because the value can hold message data.
func bsoValidateTagValue(key, value string) error {
	if r, ok := bsoFirstInvalidTagChar(value); ok {
		return fmt.Errorf("blob index tag %q value contains unsupported character %q, only alphanumerics and the characters %q are permitted", key, r, bsoTagAllowedSpecialChars)
	}
	// Only ASCII characters reach here, so byte length matches the character
	// count Azure counts against the limit.
	if l := len(value); l > bsoMaxTagValueLength {
		return fmt.Errorf("blob index tag %q value must be at most %d characters, got %d", key, bsoMaxTagValueLength, l)
	}
	return nil
}

// bsoFirstInvalidTagChar returns the first rune of s that Azure does not
// permit in a blob index tag key or value.
func bsoFirstInvalidTagChar(s string) (rune, bool) {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune(bsoTagAllowedSpecialChars, r):
		default:
			return r, true
		}
	}
	return 0, false
}

type bsoConfig struct {
	client            *azblob.Client
	Container         *service.InterpolatedString
	Path              *service.InterpolatedString
	Tags              map[string]*service.InterpolatedString
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

	if conf.Tags, err = pConf.FieldInterpolatedStringMap(bsoFieldTags); err != nil {
		return
	}
	if len(conf.Tags) > bsoMaxTagsPermitted {
		err = fmt.Errorf("at most %d blob index tags are permitted, got %d", bsoMaxTagsPermitted, len(conf.Tags))
		return
	}
	// Validate in key order so that a config with several invalid keys always
	// reports the same one.
	for _, k := range slices.Sorted(maps.Keys(conf.Tags)) {
		if err = bsoValidateTagKey(k); err != nil {
			return
		}
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
		Stable().
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
`+"`storage_account`"+` field.

== Tags

The `+"`tags`"+` field allows you to attach key/value pairs to blobs as https://learn.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs[blob index tags^], where the values support xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions]:

`+"```yaml"+`
output:
  azure_blob_storage:
    container: TODO
    path: ${!counter()}-${!timestamp_unix_nano()}.txt
    tags:
      Environment: production
      Source: ${!@kafka_topic}
`+"```"+`

Keys and values can contain only alphanumerics, spaces, and the characters `+"`+ - . / : = _`"+`. Only values support interpolation. Keys are static and are checked against Azure's limits at startup. Values are resolved and checked per message, and a message with an invalid value fails before any upload.

If authenticating with `+"`storage_sas_token`"+`, the token must include the `+"`t`"+` (tags) permission.

When `+"`blob_type`"+` is `+"`APPEND`"+` the tags describe the blob rather than any single message, so they are applied only when this output creates the blob, and are not re-sent with subsequent appends. Two consequences are worth knowing:

- A blob that already exists is never tagged, including one created by an earlier run of the pipeline. Appending to a long-lived blob therefore only picks up tags if this output created it.
- Interpolated tag values resolve from the message that creates the blob.`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewInterpolatedStringField(bsoFieldContainer).
				Description(`The container for uploading the messages to.`).
				Example(`messages-${!timestamp("2006")}`),
			service.NewInterpolatedStringField(bsoFieldPath).
				Description("The path of each message to upload.").
				Example(`${!counter()}-${!timestamp_unix_nano()}.json`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!counter()}-${!timestamp_unix_nano()}.txt`),
			service.NewInterpolatedStringMapField(bsoFieldTags).
				Description(fmt.Sprintf("Key/value pairs to store with the blob as https://learn.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs[blob index tags^]. A maximum of %d tags are permitted, tag keys must be between 1 and %d characters, and tag values must be between 0 and %d characters. Keys and values are case-sensitive and only support string values. Not supported on storage accounts with hierarchical namespace (Data Lake Gen2).", bsoMaxTagsPermitted, bsoMaxTagKeyLength, bsoMaxTagValueLength)).
				Version("4.118.0").
				Default(map[string]any{}).
				Example(map[string]any{
					"Environment": "production",
					"Source":      `${!@kafka_topic}`,
				}).
				Advanced(),
			service.NewInterpolatedStringEnumField(bsoFieldBlobType, "BLOCK", "APPEND").
				Description("Block and Append blobs are made up of blocks, and each blob can support up to 50,000 blocks. The default value is `BLOCK`.").
				ShortDescription("Block and Append blobs are made up of blocks, and each blob supports up to 50,000 blocks.").
				Advanced().
				Default("BLOCK"),
			service.NewInterpolatedStringEnumField(bsoFieldPublicAccessLevel, "PRIVATE", "BLOB", "CONTAINER").
				Description(`The container's public access level. The default value is `+"`"+`PRIVATE`+"`"+`.`).
				ShortDescription("The container's public access level.").
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

// resolveTags interpolates the configured blob index tag values against msg.
// It returns nil when no tags are configured.
func (c *bsoConfig) resolveTags(msg *service.Message) (map[string]string, error) {
	if len(c.Tags) == 0 {
		return nil, nil
	}
	tags := make(map[string]string, len(c.Tags))
	for k, v := range c.Tags {
		tagVal, err := v.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("tag %v interpolation: %w", k, err)
		}
		if err := bsoValidateTagValue(k, tagVal); err != nil {
			return nil, err
		}
		tags[k] = tagVal
	}
	return tags, nil
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

func (a *azureBlobStorageWriter) uploadBlob(ctx context.Context, containerName, blobName, blobType string, message []byte, tags map[string]string) error {
	containerClient := a.conf.client.ServiceClient().NewContainerClient(containerName)
	var err error
	if blobType == "APPEND" {
		appendBlobClient := containerClient.NewAppendBlobClient(blobName)
		_, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(message)), nil)
		if err != nil {
			if isErrorCode(err, bloberror.BlobNotFound) {
				// Another write can create the blob and append to it before this Create. Without If-None-Match,
				// Create replaces that blob and its data is lost. With it, Create fails if the blob exists. Azurite
				// returns BlobAlreadyExists (409). The REST docs give 412 for a failed If-None-Match on writes.
				//
				// AppendBlock does not support tags, so for append blobs they are
				// applied here, at creation. Tags describe the blob rather than any
				// single message, so they are deliberately not re-sent with every
				// append: that would double the request count on the hot path and
				// only ever reflect whichever message happened to be last. The
				// trade-off is that a blob this output did not create is never
				// tagged, which is documented on the tags field.
				createOpts := &appendblob.CreateOptions{
					AccessConditions: &blob.AccessConditions{
						ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)},
					},
				}
				if len(tags) > 0 {
					createOpts.Tags = tags
				}
				if _, createErr := appendBlobClient.Create(ctx, createOpts); createErr != nil && !isErrorCode(createErr, bloberror.BlobAlreadyExists) && !isErrorCode(createErr, bloberror.ConditionNotMet) {
					return fmt.Errorf("creating append blob: %w", createErr)
				}

				// Try to upload the message again now that we created the blob
				if _, err = appendBlobClient.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(message)), nil); err != nil {
					return fmt.Errorf("failed retrying to append block to blob: %w", err)
				}
			} else {
				return fmt.Errorf("appending block to blob: %w", err)
			}
		}
	} else {
		var uploadOpts *blockblob.UploadStreamOptions
		if len(tags) > 0 {
			uploadOpts = &blockblob.UploadStreamOptions{Tags: tags}
		}
		_, err = containerClient.NewBlockBlobClient(blobName).UploadStream(ctx, bytes.NewReader(message), uploadOpts)
		if err != nil {
			return fmt.Errorf("pushing block to blob: %w", err)
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

	blobTags, err := a.conf.resolveTags(msg)
	if err != nil {
		return err
	}

	if err := a.uploadBlob(ctx, containerName, blobName, blobType, mBytes, blobTags); err != nil {
		if isErrorCode(err, bloberror.ContainerNotFound) {
			var accessLevel string
			if accessLevel, err = a.conf.PublicAccessLevel.TryString(msg); err != nil {
				return fmt.Errorf("access level interpolation error: %s", err)
			}

			if err := a.createContainer(ctx, containerName, accessLevel); err != nil {
				if !isErrorCode(err, bloberror.ContainerAlreadyExists) {
					return fmt.Errorf("creating container: %s", err)
				}
			}

			if err := a.uploadBlob(ctx, containerName, blobName, blobType, mBytes, blobTags); err != nil {
				return fmt.Errorf("error retrying to upload blob: %s", err)
			}
		} else {
			return fmt.Errorf("uploading blob: %s", err)
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
