package azure

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

const (
	// Common fields for blob storage components
	bscFieldStorageAccount          = "storage_account"
	bscFieldStorageAccessKey        = "storage_access_key"
	bscFieldStorageSASToken         = "storage_sas_token"
	bscFieldStorageConnectionString = "storage_connection_string"
)

func azureComponentSpec(forBlobStorage bool) *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services", "Azure").
		Fields(
			service.NewStringField(bscFieldStorageAccount).
				Description("The storage account to access. This field is ignored if `"+bscFieldStorageConnectionString+"` is set.").
				Default(""),
			service.NewStringField(bscFieldStorageAccessKey).
				Description("The storage account access key. This field is ignored if `"+bscFieldStorageConnectionString+"` is set.").
				Default(""),
		)
	if forBlobStorage {
		spec = spec.Field(service.NewStringField(bscFieldStorageSASToken).
			Description("The storage account SAS token. This field is ignored if `" + bscFieldStorageConnectionString + "` or `" + bscFieldStorageAccessKey + "` are set.").
			Default("")).
			LintRule(`root = if this.storage_connection_string != "" && !this.storage_connection_string.contains("AccountName=") && this.storage_account == "" { [ "storage_account must be set if storage_connection_string does not contain the \"AccountName\" parameter" ] }`)
	}
	spec = spec.Fields(
		service.NewStringField(bscFieldStorageConnectionString).
			Description("A storage account connection string. This field is required if `" + bscFieldStorageAccount + "` and `" + bscFieldStorageAccessKey + "` / `" + bscFieldStorageSASToken + "` are not set.").
			Default(""),
	)
	return spec
}

func blobStorageClientFromParsed(pConf *service.ParsedConfig) (*azblob.Client, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, err
	}
	storageSASToken, err := pConf.FieldString(bscFieldStorageSASToken)
	if err != nil {
		return nil, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	return getBlobStorageClient(connectionString, storageAccount, storageAccessKey, storageSASToken)
}

func getBlobStorageClient(storageConnectionString, storageAccount, storageAccessKey, storageSASToken string) (*azblob.Client, error) {
	var client *azblob.Client
	var err error
	if len(storageConnectionString) > 0 {
		storageConnectionString := storageConnectionString
		if strings.Contains(storageConnectionString, "UseDevelopmentStorage=true;") {
			storageConnectionString = getEmulatorConnectionString("10000", "10001", "10002")
		}
		// The Shared Access Signature UI doesn't add the AccountName parameter to the Connection String for some reason...
		// However, in the Access Keys UI, the Connection String does have the AccountName parameter embedded in it.
		// I think it's worth maintaining this hack in here to help users who try to use SAS tokens in Connection String
		// format.
		if !strings.Contains(storageConnectionString, "AccountName=") {
			storageConnectionString = storageConnectionString + ";" + "AccountName=" + storageAccount
		}
		client, err = azblob.NewClientFromConnectionString(storageConnectionString, nil)
	} else if len(storageAccessKey) > 0 {
		cred, credErr := azblob.NewSharedKeyCredential(storageAccount, storageAccessKey)
		if credErr != nil {
			return nil, fmt.Errorf("error creating shared key credential: %w", credErr)
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	} else if len(storageSASToken) > 0 {
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", storageAccount, storageSASToken)
		client, err = azblob.NewClientWithNoCredential(serviceURL, nil)
	} else {
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("error getting default Azure credentials: %v", credErr)
		}
		serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", storageAccount)
		client, err = azblob.NewClient(serviceURL, cred, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	return client, err
}

// getEmulatorConnectionString returns the Azurite connection string for the provided service ports
// Details here: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#http-connection-strings
func getEmulatorConnectionString(blobServicePort, queueServicePort, tableServicePort string) string {
	return fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:%s/devstoreaccount1;QueueEndpoint=http://127.0.0.1:%s/devstoreaccount1;TableEndpoint=http://127.0.0.1:%s/devstoreaccount1;",
		blobServicePort, queueServicePort, tableServicePort,
	)
}

//------------------------------------------------------------------------------

const (
	azQueueEndpointExp  = "https://%s.queue.core.windows.net"
	devQueueEndpointExp = "http://localhost:10001/%s"
	azAccountName       = "accountname"
	azAccountKey        = "accountkey"
)

func queueServiceURLFromParsed(pConf *service.ParsedConfig) (*azqueue.ServiceURL, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	return getQueueServiceURL(storageAccount, storageAccessKey, connectionString)
}

func getQueueServiceURL(storageAccount, storageAccessKey, storageConnectionString string) (*azqueue.ServiceURL, error) {
	if storageAccount == "" && storageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	endpointExp := azQueueEndpointExp
	var err error
	if storageConnectionString != "" {
		if strings.Contains(storageConnectionString, "UseDevelopmentStorage=true;") {
			storageAccount = storage.StorageEmulatorAccountName
			storageAccessKey = storage.StorageEmulatorAccountKey
			endpointExp = devQueueEndpointExp
			if ap := os.Getenv("AZURITE_QUEUE_ENDPOINT_PORT"); ap != "" {
				endpointExp = strings.ReplaceAll(devQueueEndpointExp, "10001", ap)
			}
		} else {
			storageAccount, storageAccessKey, err = parseConnectionString(storageConnectionString)
			if err != nil {
				return nil, err
			}
			if strings.Contains(storageConnectionString, storage.StorageEmulatorAccountName) {
				endpointExp = devQueueEndpointExp
			}
		}
	}
	if storageAccount == "" {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	var credential azqueue.Credential
	if storageAccessKey != "" {
		credential, _ = azqueue.NewSharedKeyCredential(storageAccount, storageAccessKey)
	} else {
		credential = azqueue.NewAnonymousCredential()
	}

	p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{})
	endpoint, _ := url.Parse(fmt.Sprintf(endpointExp, storageAccount))
	serviceURL := azqueue.NewServiceURL(*endpoint, p)

	return &serviceURL, err
}

// parseConnectionString extracts the credentials from the connection string.
func parseConnectionString(input string) (storageAccount, storageAccessKey string, err error) {
	// build a map of connection string key/value pairs
	parts := map[string]string{}
	for _, pair := range strings.Split(input, ";") {
		if pair == "" {
			continue
		}
		equalDex := strings.IndexByte(pair, '=')
		if equalDex <= 0 {
			fmt.Println(fmt.Errorf("invalid connection segment %q", pair))
		}
		value := strings.TrimSpace(pair[equalDex+1:])
		key := strings.TrimSpace(strings.ToLower(pair[:equalDex]))
		parts[key] = value
	}
	accountName, ok := parts[azAccountName]
	if !ok {
		return "", "", errors.New("invalid connection string")
	}
	accountKey, ok := parts[azAccountKey]
	if !ok {
		return "", "", errors.New("invalid connection string")
	}
	return accountName, accountKey, nil
}

//------------------------------------------------------------------------------

func serviceClientFromParsed(pConf *service.ParsedConfig) (*aztables.ServiceClient, error) {
	connectionString, err := pConf.FieldString(bscFieldStorageConnectionString)
	if err != nil {
		return nil, err
	}
	storageAccount, err := pConf.FieldString(bscFieldStorageAccount)
	if err != nil {
		return nil, err
	}
	storageAccessKey, err := pConf.FieldString(bscFieldStorageAccessKey)
	if err != nil {
		return nil, err
	}
	if storageAccount == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	return getServiceClient(storageAccount, storageAccessKey, connectionString)
}

func getServiceClient(account, accessKey, connectionString string) (*aztables.ServiceClient, error) {
	var err error
	if account == "" && connectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var client *aztables.ServiceClient
	if connectionString != "" {
		if strings.Contains(connectionString, "UseDevelopmentStorage=true;") {
			// Only here to support legacy configs that pass UseDevelopmentStorage=true;
			// `UseDevelopmentStorage=true` is not available in the current SDK, neither `storage.NewEmulatorClient()` (which was used in the previous SDK).
			// Instead, we use the http connection string to connect to the emulator endpoints with the default table storage port.
			// https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#http-connection-strings
			client, err = aztables.NewServiceClientFromConnectionString(
				fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;TableEndpoint=http://127.0.0.1:10002/%s;",
					storage.StorageEmulatorAccountName, storage.StorageEmulatorAccountKey, storage.StorageEmulatorAccountName), &aztables.ClientOptions{})
		} else {
			client, err = aztables.NewServiceClientFromConnectionString(connectionString, &aztables.ClientOptions{})
		}
	} else {
		cred, credErr := aztables.NewSharedKeyCredential(account, accessKey)
		if credErr != nil {
			return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
		}
		client, err = aztables.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.table.core.windows.net/", account), cred, nil)
	}
	return client, err
}
