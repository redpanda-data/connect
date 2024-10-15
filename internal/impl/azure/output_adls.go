// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package azure

import (
	"context"
	"fmt"

	dlservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func adlsSpec() *service.ConfigSpec {
	return azureComponentSpec(true).
		Beta().
		Version("4.38.0").
		Summary(`Sends message parts as files to an Azure Data Lake Gen2 filesystem. Each file is uploaded with the filename specified with the `+"`"+adlsFieldPath+"`"+` field.`).
		Description(`
In order to have a different path for each file you should use function
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
			service.NewInterpolatedStringField(adlsFieldFilesystem).
				Description("The ADLS filesystem name for uploading the messages to.").
				Example(`messages-${!timestamp("2006")}`),
			service.NewInterpolatedStringField(adlsFieldPath).
				Description("The path of each message to upload within the filesystem.").
				Example(`${!count("files")}-${!timestamp_unix_nano()}.json`).
				Example(`${!meta("kafka_key")}.json`).
				Example(`${!json("doc.namespace")}/${!json("doc.id")}.json`).
				Default(`${!count("files")}-${!timestamp_unix_nano()}.txt`),
			service.NewOutputMaxInFlightField(),
		)
}

const (
	// Azure Data Lake Storage Output Fields
	adlsFieldFilesystem = "filesystem"
	adlsFieldPath       = "path"
)

type adlsConfig struct {
	client     *dlservice.Client
	path       *service.InterpolatedString
	filesystem *service.InterpolatedString
}

func init() {
	err := service.RegisterOutput("azure_data_lake_gen2", adlsSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, mif int, err error) {
			var pConf *adlsConfig
			if pConf, err = adlsConfigFromParsed(conf); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newAzureADLSWriter(pConf, mgr.Logger()); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

func adlsConfigFromParsed(pConf *service.ParsedConfig) (*adlsConfig, error) {
	var conf adlsConfig
	var err error
	conf.filesystem, err = pConf.FieldInterpolatedString(adlsFieldFilesystem)
	if err != nil {
		return nil, err
	}
	conf.path, err = pConf.FieldInterpolatedString(adlsFieldPath)
	if err != nil {
		return nil, err
	}
	var isFilesystemSASToken bool
	conf.client, isFilesystemSASToken, err = adlsClientFromParsed(pConf, conf.filesystem)
	if err != nil {
		return nil, err
	}
	if isFilesystemSASToken {
		// if using a container SAS token, the container is already implicit
		conf.filesystem, _ = service.NewInterpolatedString("")
	}
	return &conf, nil
}

func newAzureADLSWriter(conf *adlsConfig, log *service.Logger) (*azureADLSWriter, error) {
	return &azureADLSWriter{
		conf: conf,
		log:  log,
	}, nil
}

type azureADLSWriter struct {
	conf *adlsConfig
	log  *service.Logger
}

func (a *azureADLSWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureADLSWriter) Write(ctx context.Context, msg *service.Message) error {
	fsName, err := a.conf.filesystem.TryString(msg)
	if err != nil {
		return fmt.Errorf("interpolating filesystem name: %w", err)
	}
	path, err := a.conf.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("interpolating file path: %w", err)
	}
	mBytes, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("reading message body: %w", err)
	}

	fileClient := a.conf.client.NewFileSystemClient(fsName).NewFileClient(path)
	_, err = fileClient.Create(ctx, nil)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	err = fileClient.UploadBuffer(ctx, mBytes, nil)
	if err != nil {
		return fmt.Errorf("uploading message body: %w", err)
	}
	return nil
}

func (a *azureADLSWriter) Close(ctx context.Context) error {
	return nil
}
