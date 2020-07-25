// +build !wasm

package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AzureTableStorage is a benthos writer. Type implementation that writes messages to an
// Azure Table Storage table.
type AzureTableStorage struct {
	conf         AzureTableStorageConfig
	tableName    field.Expression
	partitionKey field.Expression
	rowKey       field.Expression
	properties   map[string]field.Expression
	client       storage.TableServiceClient
	timeout      time.Duration
	log          log.Modular
	stats        metrics.Type
}

// NewAzureTableStorage creates a new Azure Table Storage writer Type.
func NewAzureTableStorage(
	conf AzureTableStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*AzureTableStorage, error) {
	var timeout time.Duration
	var err error
	if tout := conf.Timeout; len(tout) > 0 {
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	if len(conf.StorageAccount) == 0 {
		return nil, fmt.Errorf("invalid azure storage account")
	}
	basicClient, err := storage.NewBasicClient(conf.StorageAccount, conf.StorageAccessKey)
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	a := &AzureTableStorage{
		conf:    conf,
		log:     log,
		stats:   stats,
		timeout: timeout,
		client:  basicClient.GetTableService(),
	}
	if a.tableName, err = field.New(conf.TableName); err != nil {
		return nil, fmt.Errorf("failed to parse table name expression: %v", err)
	}
	if a.partitionKey, err = field.New(conf.PartitionKey); err != nil {
		return nil, fmt.Errorf("failed to parse partition key expression: %v", err)
	}
	if a.rowKey, err = field.New(conf.RowKey); err != nil {
		return nil, fmt.Errorf("failed to parse row key expression: %v", err)
	}
	a.properties = make(map[string]field.Expression)
	for property, value := range conf.Properties {
		if a.properties[property], err = field.New(value); err != nil {
			return nil, fmt.Errorf("failed to parse property expression: %v", err)
		}
	}

	return a, nil
}

// ConnectWithContext attempts to establish a connection to the target Table Storage Account.
func (a *AzureTableStorage) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target Table Storage Account.
func (a *AzureTableStorage) Connect() error {
	return nil
}

// Write attempts to write message contents to a target Azure Table Storage container as files.
func (a *AzureTableStorage) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target storage account as files.
func (a *AzureTableStorage) WriteWithContext(wctx context.Context, msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		tableName := a.tableName.String(i, msg)
		partitionKey := a.partitionKey.String(i, msg)
		rowKey := a.rowKey.String(i, msg)

		table := a.client.GetTableReference(tableName)

		entity := table.GetEntityReference(partitionKey, rowKey)
		entity.TimeStamp = time.Now()
		jsonMap := make(map[string]interface{})

		if len(a.properties) == 0 {
			err := json.Unmarshal(p.Get(), &jsonMap)
			if err != nil {
				a.log.Errorf("error unmarshalling message: %v.", err)
			}
			for property, v := range jsonMap {
				switch v.(type) {
				case []interface{}, map[string]interface{}:
					m, err := json.Marshal(v)
					if err != nil {
						a.log.Errorf("error marshalling property: %v.", property)
					}
					jsonMap[property] = string(m)
				}
			}
		} else {
			for property, value := range a.properties {
				jsonMap[property] = value.String(i, msg)
			}
		}
		entity.Properties = jsonMap
		if err := a.insert(a.conf.InsertType, entity); err != nil {
			if cerr, ok := err.(storage.AzureStorageServiceError); ok {
				if cerr.Code == "TableNotFound" {
					if cerr := table.Create(uint(10), storage.FullMetadata, nil); cerr != nil {
						a.log.Errorf("error creating table: %v.", cerr)
					}
					// retry
					err = a.insert(a.conf.InsertType, entity)
				}
			}
			return err
		}
		return nil
	})
}

func (a *AzureTableStorage) insert(insertType string, entity *storage.Entity) error {
	switch insertType {
	case "INSERT":
		return entity.Insert(storage.FullMetadata, nil)
	case "INSERT_MERGE":
		return entity.InsertOrMerge(nil)
	case "INSERT_REPLACE":
		return entity.InsertOrReplace(nil)
	default:
		return fmt.Errorf("invalid insert type")
	}
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AzureTableStorage) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AzureTableStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
