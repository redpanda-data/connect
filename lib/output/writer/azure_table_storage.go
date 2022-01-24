//go:build !wasm
// +build !wasm

package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AzureTableStorage is a benthos writer. Type implementation that writes messages to an
// Azure Table Storage table.
type AzureTableStorage struct {
	conf         AzureTableStorageConfig
	tableName    *field.Expression
	partitionKey *field.Expression
	rowKey       *field.Expression
	properties   map[string]*field.Expression
	client       storage.TableServiceClient
	timeout      time.Duration
	log          log.Modular
	stats        metrics.Type
}

// NewAzureTableStorageV2 creates a new Azure Table Storage writer Type.
func NewAzureTableStorageV2(
	conf AzureTableStorageConfig,
	mgr types.Manager,
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
	if conf.StorageAccount == "" && conf.StorageConnectionString == "" {
		return nil, errors.New("invalid azure storage account credentials")
	}
	var client storage.Client
	if conf.StorageConnectionString != "" {
		if strings.Contains(conf.StorageConnectionString, "UseDevelopmentStorage=true;") {
			client, err = storage.NewEmulatorClient()
		} else {
			client, err = storage.NewClientFromConnectionString(conf.StorageConnectionString)
		}
	} else {
		client, err = storage.NewBasicClient(conf.StorageAccount, conf.StorageAccessKey)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	a := &AzureTableStorage{
		conf:    conf,
		log:     log,
		stats:   stats,
		timeout: timeout,
		client:  client.GetTableService(),
	}
	if a.tableName, err = interop.NewBloblangField(mgr, conf.TableName); err != nil {
		return nil, fmt.Errorf("failed to parse table name expression: %v", err)
	}
	if a.partitionKey, err = interop.NewBloblangField(mgr, conf.PartitionKey); err != nil {
		return nil, fmt.Errorf("failed to parse partition key expression: %v", err)
	}
	if a.rowKey, err = interop.NewBloblangField(mgr, conf.RowKey); err != nil {
		return nil, fmt.Errorf("failed to parse row key expression: %v", err)
	}
	a.properties = make(map[string]*field.Expression)
	for property, value := range conf.Properties {
		if a.properties[property], err = interop.NewBloblangField(mgr, value); err != nil {
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
	writeReqs := make(map[string]map[string][]*storage.Entity)
	if err := IterateBatchedSend(msg, func(i int, p types.Part) error {
		entity := &storage.Entity{}
		tableName := a.tableName.String(i, msg)
		partitionKey := a.partitionKey.String(i, msg)
		entity.PartitionKey = a.partitionKey.String(i, msg)
		entity.RowKey = a.rowKey.String(i, msg)
		entity.Properties = a.getProperties(i, p, msg)
		if writeReqs[tableName] == nil {
			writeReqs[tableName] = make(map[string][]*storage.Entity)
		}
		writeReqs[tableName][partitionKey] = append(writeReqs[tableName][partitionKey], entity)
		return nil
	}); err != nil {
		return err
	}
	return a.writeBatches(writeReqs)
}

func (a *AzureTableStorage) getProperties(i int, p types.Part, msg types.Message) map[string]interface{} {
	properties := make(map[string]interface{})
	if len(a.properties) == 0 {
		err := json.Unmarshal(p.Get(), &properties)
		if err != nil {
			a.log.Errorf("error unmarshalling message: %v.", err)
		}
		for property, v := range properties {
			switch v.(type) {
			case []interface{}, map[string]interface{}:
				m, err := json.Marshal(v)
				if err != nil {
					a.log.Errorf("error marshaling property: %v.", property)
				}
				properties[property] = string(m)
			}
		}
	} else {
		for property, value := range a.properties {
			properties[property] = value.String(i, msg)
		}
	}
	return properties
}

func (a *AzureTableStorage) writeBatches(writeReqs map[string]map[string][]*storage.Entity) error {
	for tn, pks := range writeReqs {
		table := a.client.GetTableReference(tn)
		for _, entities := range pks {
			tableBatch := table.NewBatch()
			ne := len(entities)
			for i, entity := range entities {
				entity.Table = table
				if err := a.addToBatch(tableBatch, a.conf.InsertType, entity); err != nil {
					return err
				}
				if reachedBatchLimit(i) || isLastEntity(i, ne) {
					if err := a.executeBatch(table, tableBatch); err != nil {
						return err
					}
					tableBatch = table.NewBatch()
				}
			}
		}
	}
	return nil
}

func (a *AzureTableStorage) executeBatch(table *storage.Table, tableBatch *storage.TableBatch) error {
	if err := tableBatch.ExecuteBatch(); err != nil {
		if tableDoesNotExist(err) {
			if cerr := table.Create(uint(10), storage.FullMetadata, nil); cerr != nil {
				return cerr
			}
			err = tableBatch.ExecuteBatch()
		}
		return err
	}
	return nil
}

func tableDoesNotExist(err error) bool {
	if cerr, ok := err.(storage.AzureStorageServiceError); ok {
		return cerr.Code == "TableNotFound"
	}
	return false
}

func isLastEntity(i, ne int) bool {
	return i+1 == ne
}

func reachedBatchLimit(i int) bool {
	const batchSizeLimit = 100
	return (i+1)%batchSizeLimit == 0
}

func (a *AzureTableStorage) addToBatch(tableBatch *storage.TableBatch, insertType string, entity *storage.Entity) error {
	switch insertType {
	case "INSERT":
		tableBatch.InsertEntity(entity)
	case "INSERT_MERGE":
		tableBatch.InsertOrMergeEntity(entity, true)
	case "INSERT_REPLACE":
		tableBatch.InsertOrReplaceEntity(entity, true)
	default:
		return fmt.Errorf("invalid insert type")
	}
	return nil
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
