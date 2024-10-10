package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// File Share output fields
	fsFieldShareName = "share_name"
	fsFieldPath      = "path"
)

type fsConfig struct {
	client              *share.Client
	storageSASToken     string
	storageAccount      string
	usesStorageSASToken bool
	ShareName           *service.InterpolatedString
	Path                *service.InterpolatedString
}

func fsConfigFromParsed(pConf *service.ParsedConfig) (conf fsConfig, err error) {
	if conf.ShareName, err = pConf.FieldInterpolatedString(fsFieldShareName); err != nil {
		return
	}
	var usesShareSASToken bool
	c, err := conf.ShareName.TryString(service.NewMessage([]byte("")))
	if err != nil {
		return
	}
	if conf.client, usesShareSASToken, conf.usesStorageSASToken, conf.storageAccount, conf.storageSASToken, err =
		fileShareClientFromParsed(pConf, c); err != nil {
		return
	}
	if usesShareSASToken {
		// if using a share SAS token, the share name is already implicit
		conf.ShareName, _ = service.NewInterpolatedString("")
	}
	if conf.Path, err = pConf.FieldInterpolatedString(fsFieldPath); err != nil {
		return
	}
	return
}

func fsSpec() *service.ConfigSpec {
	return azureComponentSpec().
		Beta().
		Version("4.38.0").
		Summary(`Sends message parts as objects to an Azure File Share. Each object is uploaded with the filename specified with the `+"`path`"+` field.`).
		Description(`
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
`+"`storage_account`"+` field.`).
		Fields(
			service.NewInterpolatedStringField(fsFieldShareName).
				Description("The file share for uploading the files to. It will be created if it doesn't already exist and the credentials have the necessary permissions.").
				Example(`foo-share-${!timestamp("2006")}`),
			service.NewInterpolatedStringField(fsFieldPath).
				Description("The path of each file to upload.").
				Example(`foo-${!timestamp_unix_nano()}.json`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("azure_file_share", fsSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, mif int, err error) {
			var pConf fsConfig
			if pConf, err = fsConfigFromParsed(conf); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newAzureFileShareWriter(pConf, mgr.Logger()); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

type azureFileShareWriter struct {
	conf fsConfig
	log  *service.Logger
}

func newAzureFileShareWriter(conf fsConfig, log *service.Logger) (*azureFileShareWriter, error) {
	a := &azureFileShareWriter{
		conf: conf,
		log:  log,
	}
	return a, nil
}

func (a *azureFileShareWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureFileShareWriter) uploadFile(ctx context.Context, shareClient *share.Client, path string, message []byte) error {
	directory := filepath.Dir(path)
	filename := filepath.Base(path)
	dirClient := shareClient.NewDirectoryClient(directory)
	fClient := dirClient.NewFileClient(filename)
	if _, err := fClient.Create(ctx, int64(len(message)), nil); err != nil {
		if isFileErrorCode(err, fileerror.ParentNotFound) {
			if _, err := dirClient.Create(ctx, nil); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	reader := bytes.NewReader(message)
	buffer := make([]byte, len(message))
	if _, err := reader.Read(buffer); err != nil {
		return err
	}
	if err := fClient.UploadBuffer(ctx, buffer, nil); err != nil {
		return err
	}
	return nil
}

func (a *azureFileShareWriter) createFileShare(ctx context.Context, client *share.Client) error {
	_, err := client.Create(ctx, &share.CreateOptions{})
	return err
}

func (a *azureFileShareWriter) getStorageSASURL(shareName string) string {
	return fmt.Sprintf("%s/%s%s", fmt.Sprintf(fileEndpointExp, a.conf.storageAccount), shareName, a.conf.storageSASToken)
}

func (a *azureFileShareWriter) Write(ctx context.Context, msg *service.Message) error {
	shareName, err := a.conf.ShareName.TryString(msg)
	if err != nil {
		return fmt.Errorf("file share name interpolation error: %s", err)
	}
	var shareClient = a.conf.client
	if a.conf.usesStorageSASToken && shareName != "" {
		if shareClient, err = share.NewClientWithNoCredential(a.getStorageSASURL(shareName), nil); err != nil {
			return fmt.Errorf("error getting SAS url with storage SAS token: %v", err)
		}
	}
	fileName, err := a.conf.Path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %v", err)
	}
	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}
	if err := a.uploadFile(ctx, shareClient, fileName, mBytes); err != nil {
		if isFileErrorCode(err, fileerror.ShareNotFound) {
			if err := a.createFileShare(ctx, shareClient); err != nil {
				if !isFileErrorCode(err, fileerror.ShareAlreadyExists) {
					return fmt.Errorf("failed to create file share: %s", err)
				}
			}
			if err := a.uploadFile(ctx, shareClient, fileName, mBytes); err != nil {
				return fmt.Errorf("error retrying to upload file: %s", err)
			}
		} else if isFileErrorCode(err, fileerror.ResourceNotFound) {
			if err := a.uploadFile(ctx, shareClient, fileName, mBytes); err != nil {
				return fmt.Errorf("error retrying to upload file after creating parent: %s", err)
			}
		} else {
			return fmt.Errorf("failed to upload file: %s", err)
		}
	}
	return nil
}

func (a *azureFileShareWriter) Close(context.Context) error {
	return nil
}

func isFileErrorCode(err error, code fileerror.Code) bool {
	var rerr *azcore.ResponseError
	if ok := errors.As(err, &rerr); ok {
		return rerr.ErrorCode == string(code)
	}
	return false
}
