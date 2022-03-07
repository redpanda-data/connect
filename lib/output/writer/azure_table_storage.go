//go:build !wasm
// +build !wasm

package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
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
	client       *aztables.ServiceClient
	timeout      time.Duration
	log          log.Modular
	stats        metrics.Type
}

// NewAzureTableStorage creates a new Azure Table Storage writer Type.
//
// Deprecated: use the V2 API instead.
func NewAzureTableStorage(
	conf AzureTableStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (*AzureTableStorage, error) {
	return NewAzureTableStorageV2(conf, types.NoopMgr(), log, stats)
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
	var client *aztables.ServiceClient
	if conf.StorageConnectionString != "" {
		if strings.Contains(conf.StorageConnectionString, "UseDevelopmentStorage=true;") {
			// Only here to support legacy configs that pass UseDevelopmentStorage=true;
			// `UseDevelopmentStorage=true` is not available in the current SDK, neither `storage.NewEmulatorClient()` (which was used in the previous SDK).
			// Instead, we use the http connection string to connect to the emulator endpoints with the default table storage port.
			// https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#http-connection-strings
			client, err = aztables.NewServiceClientFromConnectionString("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;", nil)
		} else {
			client, err = aztables.NewServiceClientFromConnectionString(conf.StorageConnectionString, nil)
		}
	} else {
		cred, credErr := aztables.NewSharedKeyCredential(conf.StorageAccount, conf.StorageAccessKey)
		if credErr != nil {
			return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
		}
		client, err = aztables.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.table.core.windows.net/", conf.StorageAccount), cred, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	a := &AzureTableStorage{
		conf:    conf,
		log:     log,
		stats:   stats,
		timeout: timeout,
		client:  client,
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
func (a *AzureTableStorage) WriteWithContext(ctx context.Context, msg types.Message) error {
	writeReqs := make(map[string]map[string][]*aztables.EDMEntity)
	if err := IterateBatchedSend(msg, func(i int, p types.Part) error {
		entity := &aztables.EDMEntity{}
		tableName := a.tableName.String(i, msg)
		partitionKey := a.partitionKey.String(i, msg)
		entity.PartitionKey = a.partitionKey.String(i, msg)
		entity.RowKey = a.rowKey.String(i, msg)
		entity.Properties = a.getProperties(i, p, msg)
		if writeReqs[tableName] == nil {
			writeReqs[tableName] = make(map[string][]*aztables.EDMEntity)
		}
		writeReqs[tableName][partitionKey] = append(writeReqs[tableName][partitionKey], entity)
		return nil
	}); err != nil {
		return err
	}
	return a.execBatch(writeReqs)
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

func (a *AzureTableStorage) execBatch(writeReqs map[string]map[string][]*aztables.EDMEntity) error {
	for tn, pks := range writeReqs {
		table := a.client.NewClient(tn)
		_, err := table.Create(context.Background(), nil)
		if !tableExists(err) {
			return err
		}
		for _, entities := range pks {
			var batch []aztables.TransactionAction
			ne := len(entities)
			for i, entity := range entities {
				batch, err = a.addToBatch(batch, a.conf.InsertType, entity)
				if err != nil {
					return err
				}
				if reachedBatchLimit(i) || isLastEntity(i, ne) {
					if _, err := table.SubmitTransaction(context.Background(), batch, nil); err != nil {
						return err
					}
					batch = nil
				}
			}
		}
	}
	return nil
}

func tableExists(err error) bool {
	if err == nil {
		return false
	}
	var azErr *azcore.ResponseError
	if errors.As(err, &azErr) {
		return azErr.StatusCode == http.StatusConflict
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

func (a *AzureTableStorage) addToBatch(batch []aztables.TransactionAction, insertType string, entity *aztables.EDMEntity) ([]aztables.TransactionAction, error) {
	appendFunc := func(b []aztables.TransactionAction, t aztables.TransactionType, e *aztables.EDMEntity) ([]aztables.TransactionAction, error) {
		// marshal entity
		m, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("error marshalling entity: %v", err)
		}
		b = append(b, aztables.TransactionAction{
			ActionType: t,
			Entity:     m,
		})
		return b, nil
	}
	var err error
	switch strings.ToUpper(insertType) {
	case "ADD":
		batch, err = appendFunc(batch, aztables.Add, entity)
	case "INSERT", "INSERT_MERGE", "INSERTMERGE":
		batch, err = appendFunc(batch, aztables.InsertMerge, entity)
	case "INSERT_REPLACE", "INSERTREPLACE":
		batch, err = appendFunc(batch, aztables.InsertReplace, entity)
	case "UPDATE", "UPDATE_MERGE", "UPDATEMERGE":
		batch, err = appendFunc(batch, aztables.UpdateMerge, entity)
	case "UPDATE_REPLACE", "UPDATEREPLACE":
		batch, err = appendFunc(batch, aztables.UpdateReplace, entity)
	case "DELETE":
		batch, err = appendFunc(batch, aztables.Delete, entity)
	default:
		return batch, fmt.Errorf("invalid insert type")
	}
	return batch, err
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
