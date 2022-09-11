package azure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newAzureTableStorageOutput), docs.ComponentSpec{
		Name:    "azure_table_storage",
		Status:  docs.StatusBeta,
		Version: "3.36.0",
		Summary: `Stores message parts in an Azure Table Storage table.`,
		Description: output.Description(true, true, `
Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

In order to set the `+"`table_name`"+`,  `+"`partition_key`"+` and `+"`row_key`"+` you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are calculated per message of a batch.

If the `+"`properties`"+` are not set in the config, all the `+"`json`"+` fields are marshaled and stored in the table, which will be created if it does not exist.

The `+"`object`"+` and `+"`array`"+` fields are marshaled as strings. e.g.:

The JSON message:
`+"```json"+`
{
  "foo": 55,
  "bar": {
    "baz": "a",
    "bez": "b"
  },
  "diz": ["a", "b"]
}
`+"```"+`

Will store in the table the following properties:
`+"```yml"+`
foo: '55'
bar: '{ "baz": "a", "bez": "b" }'
diz: '["a", "b"]'
`+"```"+`

It's also possible to use function interpolations to get or transform the properties values, e.g.:

`+"```yml"+`
properties:
  device: '${! json("device") }'
  timestamp: '${! json("timestamp") }'
`+"```"+``),
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
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldString("table_name", "The table to store messages into.",
				`${!meta("kafka_topic")}`,
			).IsInterpolated(),
			docs.FieldString("partition_key", "The partition key.",
				`${!json("date")}`,
			).IsInterpolated(),
			docs.FieldString("row_key", "The row key.",
				`${!json("device")}-${!uuid_v4()}`,
			).IsInterpolated(),
			docs.FieldString("properties", "A map of properties to store into the table.").IsInterpolated().Map(),
			docs.FieldString("insert_type", "Type of insert operation").HasOptions(
				"INSERT", "INSERT_MERGE", "INSERT_REPLACE",
			).IsInterpolated().Advanced(),
			docs.FieldInt("max_in_flight",
				"The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldString("timeout", "The maximum period to wait on an upload before abandoning it and reattempting.").Advanced(),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewAzureTableStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newAzureTableStorageOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	tableStorage, err := newAzureTableStorageWriter(conf.AzureTableStorage, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("azure_table_storage", conf.AzureTableStorage.MaxInFlight, tableStorage, mgr)
	if err != nil {
		return nil, err
	}
	return batcher.NewFromConfig(conf.AzureTableStorage.Batching, w, mgr)
}

type azureTableStorageWriter struct {
	conf         output.AzureTableStorageConfig
	tableName    *field.Expression
	partitionKey *field.Expression
	rowKey       *field.Expression
	properties   map[string]*field.Expression
	client       *aztables.ServiceClient
	timeout      time.Duration
	log          log.Modular
}

func newAzureTableStorageWriter(conf output.AzureTableStorageConfig, mgr bundle.NewManagement) (*azureTableStorageWriter, error) {
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
	a := &azureTableStorageWriter{
		conf:    conf,
		log:     mgr.Logger(),
		timeout: timeout,
		client:  client,
	}
	if a.tableName, err = mgr.BloblEnvironment().NewField(conf.TableName); err != nil {
		return nil, fmt.Errorf("failed to parse table name expression: %v", err)
	}
	if a.partitionKey, err = mgr.BloblEnvironment().NewField(conf.PartitionKey); err != nil {
		return nil, fmt.Errorf("failed to parse partition key expression: %v", err)
	}
	if a.rowKey, err = mgr.BloblEnvironment().NewField(conf.RowKey); err != nil {
		return nil, fmt.Errorf("failed to parse row key expression: %v", err)
	}
	a.properties = make(map[string]*field.Expression)
	for property, value := range conf.Properties {
		if a.properties[property], err = mgr.BloblEnvironment().NewField(value); err != nil {
			return nil, fmt.Errorf("failed to parse property expression: %v", err)
		}
	}

	return a, nil
}

func (a *azureTableStorageWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureTableStorageWriter) WriteBatch(wctx context.Context, msg message.Batch) error {
	writeReqs := make(map[string]map[string][]*aztables.EDMEntity)
	if err := output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
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
	return a.execBatch(wctx, writeReqs)
}

func (a *azureTableStorageWriter) getProperties(i int, p *message.Part, msg message.Batch) map[string]interface{} {
	properties := make(map[string]interface{})
	if len(a.properties) == 0 {
		err := json.Unmarshal(p.AsBytes(), &properties)
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

func (a *azureTableStorageWriter) execBatch(ctx context.Context, writeReqs map[string]map[string][]*aztables.EDMEntity) error {
	for tn, pks := range writeReqs {
		table := a.client.NewClient(tn)
		var err error
		for _, entities := range pks {
			var batch []aztables.TransactionAction
			ne := len(entities)
			for i, entity := range entities {
				batch, err = a.addToBatch(batch, a.conf.InsertType, entity)
				if err != nil {
					return err
				}
				if reachedBatchLimit(i) || isLastEntity(i, ne) {
					if _, err = table.SubmitTransaction(ctx, batch, nil); err != nil {
						tErr, ok := err.(*azcore.ResponseError)
						if !ok {
							return err
						}
						if !strings.Contains(tErr.Error(), "TableNotFound") {
							return err
						}
						if _, err = table.Create(ctx, nil); err != nil {
							return err
						}
						if _, err = table.SubmitTransaction(ctx, batch, nil); err != nil {
							return err
						}
					}
					batch = nil
				}
			}
		}
	}
	return nil
}

func isLastEntity(i, ne int) bool {
	return i+1 == ne
}

func reachedBatchLimit(i int) bool {
	const batchSizeLimit = 100
	return (i+1)%batchSizeLimit == 0
}

func (a *azureTableStorageWriter) addToBatch(batch []aztables.TransactionAction, insertType string, entity *aztables.EDMEntity) ([]aztables.TransactionAction, error) {
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
		batch, err = appendFunc(batch, aztables.TransactionTypeAdd, entity)
	case "INSERT", "INSERT_MERGE", "INSERTMERGE":
		batch, err = appendFunc(batch, aztables.TransactionTypeInsertMerge, entity)
	case "INSERT_REPLACE", "INSERTREPLACE":
		batch, err = appendFunc(batch, aztables.TransactionTypeInsertReplace, entity)
	case "UPDATE", "UPDATE_MERGE", "UPDATEMERGE":
		batch, err = appendFunc(batch, aztables.TransactionTypeUpdateMerge, entity)
	case "UPDATE_REPLACE", "UPDATEREPLACE":
		batch, err = appendFunc(batch, aztables.TransactionTypeUpdateReplace, entity)
	case "DELETE":
		batch, err = appendFunc(batch, aztables.TransactionTypeDelete, entity)
	default:
		return batch, fmt.Errorf("invalid insert type")
	}
	return batch, err
}

func (a *azureTableStorageWriter) Close(context.Context) error {
	return nil
}
