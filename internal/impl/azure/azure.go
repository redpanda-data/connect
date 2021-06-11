package azure

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

const (
	azQueueEndpointExp  = "https://%s.queue.core.windows.net"
	devQueueEndpointExp = "http://localhost:10001/%s"
	azAccountName       = "accountname"
	azAccountKey        = "accountkey"
	devAccountName      = "devstoreaccount1"
	devAccountKey       = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// GetQueueServiceURL creates an Azure Queue URL from storage fields.
func GetQueueServiceURL(storageAccount, storageAccessKey, storageConnectionString string) (*azqueue.ServiceURL, error) {
	if storageAccount == "" && storageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var endpointExp = azQueueEndpointExp
	var err error
	if storageConnectionString != "" {
		if strings.Contains(storageConnectionString, "UseDevelopmentStorage=true;") {
			storageAccount = devAccountName
			storageAccessKey = devAccountKey
			endpointExp = devQueueEndpointExp
			if ap := os.Getenv("AZURITE_QUEUE_ENDPOINT_PORT"); ap != "" {
				endpointExp = strings.ReplaceAll(devQueueEndpointExp, "10001", ap)
			}
		} else {
			storageAccount, storageAccessKey, err = parseConnectionString(storageConnectionString)
			if err != nil {
				return nil, err
			}
			if strings.Contains(storageConnectionString, devAccountName) {
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
