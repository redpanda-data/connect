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

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Table Storage Output Fields
	tsoFieldTableName       = "table_name"
	tsoFieldPartitionKey    = "partition_key"
	tsoFieldRowKey          = "row_key"
	tsoFieldProperties      = "properties"
	tsoFieldInsertType      = "insert_type"
	tsoFieldTransactionType = "transaction_type"
	tsoFieldTimeout         = "timeout"
	tsoFieldBatching        = "batching"
)

type tsoConfig struct {
	client          *aztables.ServiceClient
	TableName       *service.InterpolatedString
	PartitionKey    *service.InterpolatedString
	RowKey          *service.InterpolatedString
	Properties      map[string]*service.InterpolatedString
	TransactionType *service.InterpolatedString
	Timeout         time.Duration
}

func tsoConfigFromParsed(pConf *service.ParsedConfig) (conf tsoConfig, err error) {
	if conf.client, err = tablesServiceClientFromParsed(pConf); err != nil {
		return
	}
	if conf.TableName, err = pConf.FieldInterpolatedString(tsoFieldTableName); err != nil {
		return
	}
	if conf.PartitionKey, err = pConf.FieldInterpolatedString(tsoFieldPartitionKey); err != nil {
		return
	}
	if conf.RowKey, err = pConf.FieldInterpolatedString(tsoFieldRowKey); err != nil {
		return
	}
	if conf.Properties, err = pConf.FieldInterpolatedStringMap(tsoFieldProperties); err != nil {
		return
	}
	if iType, _ := pConf.FieldString(tsoFieldInsertType); iType != "" {
		if conf.TransactionType, err = pConf.FieldInterpolatedString(tsoFieldInsertType); err != nil {
			return
		}
	} else if conf.TransactionType, err = pConf.FieldInterpolatedString(tsoFieldTransactionType); err != nil {
		return
	}
	if conf.Timeout, err = pConf.FieldDuration(tsoFieldTimeout); err != nil {
		return
	}
	return
}

func tsoSpec() *service.ConfigSpec {
	return azureComponentSpec(false).
		Beta().
		Version("3.36.0").
		Summary(`Stores messages in an Azure Table Storage table.`).
		Description(`
Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

In order to set the `+"`table_name`"+`,  `+"`partition_key`"+` and `+"`row_key`"+` you can use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here], which are calculated per message of a batch.

If the `+"`properties`"+` are not set in the config, all the `+"`json`"+` fields are marshalled and stored in the table, which will be created if it does not exist.

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
`+"```"+``+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewInterpolatedStringField(tsoFieldTableName).
				Description("The table to store messages into.").
				Example(`${! meta("kafka_topic") }`).Example(`${! json("table") }`),
			service.NewInterpolatedStringField(tsoFieldPartitionKey).
				Description("The partition key.").
				Example(`${! json("date") }`).
				Default(""),
			service.NewInterpolatedStringField(tsoFieldRowKey).
				Description("The row key.").
				Example(`${! json("device")}-${!uuid_v4() }`).
				Default(""),
			service.NewInterpolatedStringMapField(tsoFieldProperties).
				Description("A map of properties to store into the table.").
				Default(map[string]any{}),
			service.NewInterpolatedStringEnumField(tsoFieldInsertType, `INSERT`, `INSERT_MERGE`, `INSERT_REPLACE`).
				Description("Type of insert operation. Valid options are `INSERT`, `INSERT_MERGE` and `INSERT_REPLACE`").
				Example(`${! json("operation") }`).Example(`${! meta("operation") }`).Example(`INSERT`).
				Advanced().Deprecated().
				Default(""),
			service.NewInterpolatedStringEnumField(tsoFieldTransactionType, `INSERT`, `INSERT_MERGE`, `INSERT_REPLACE`, `UPDATE_MERGE`, `UPDATE_REPLACE`, `DELETE`).
				Description("Type of transaction operation.").
				Example(`${! json("operation") }`).Example(`${! meta("operation") }`).Example(`INSERT`).
				Advanced().
				Default("INSERT"),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of parallel message batches to have in flight at any given time."),
			service.NewDurationField(tsoFieldTimeout).
				Description("The maximum period to wait on an upload before abandoning it and reattempting.").
				Advanced().Default("5s"),
			service.NewBatchPolicyField(tsoFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("azure_table_storage", tsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batcher service.BatchPolicy, mif int, err error) {
			var pConf tsoConfig
			if pConf, err = tsoConfigFromParsed(conf); err != nil {
				return
			}
			if batcher, err = conf.FieldBatchPolicy(tsoFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newAzureTableStorageWriter(pConf, mgr); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

type azureTableStorageWriter struct {
	conf tsoConfig
	log  *service.Logger
}

func newAzureTableStorageWriter(conf tsoConfig, mgr *service.Resources) (*azureTableStorageWriter, error) {
	a := &azureTableStorageWriter{
		conf: conf,
		log:  mgr.Logger(),
	}
	return a, nil
}

func (a *azureTableStorageWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureTableStorageWriter) WriteBatch(wctx context.Context, batch service.MessageBatch) error {
	writeReqs := make(map[string]map[string]map[string][]*aztables.EDMEntity)
	if err := batch.WalkWithBatchedErrors(func(i int, p *service.Message) error {
		entity := &aztables.EDMEntity{}
		transactionType, err := batch.TryInterpolatedString(i, a.conf.TransactionType)
		if err != nil {
			return fmt.Errorf("transaction type interpolation error: %w", err)
		}
		tableName, err := batch.TryInterpolatedString(i, a.conf.TableName)
		if err != nil {
			return fmt.Errorf("table name interpolation error: %w", err)
		}
		partitionKey, err := batch.TryInterpolatedString(i, a.conf.PartitionKey)
		if err != nil {
			return fmt.Errorf("partition key interpolation error: %w", err)
		}
		entity.PartitionKey = partitionKey
		if entity.RowKey, err = batch.TryInterpolatedString(i, a.conf.RowKey); err != nil {
			return fmt.Errorf("row key interpolation error: %w", err)
		}
		if entity.Properties, err = a.getProperties(i, p, batch); err != nil {
			return err
		}
		if writeReqs[tableName] == nil {
			writeReqs[tableName] = make(map[string]map[string][]*aztables.EDMEntity)
		}
		if writeReqs[tableName][partitionKey] == nil {
			writeReqs[tableName][partitionKey] = make(map[string][]*aztables.EDMEntity)
		}
		writeReqs[tableName][partitionKey][transactionType] = append(writeReqs[tableName][partitionKey][transactionType], entity)
		return nil
	}); err != nil {
		return err
	}
	return a.execBatch(wctx, writeReqs)
}

func (a *azureTableStorageWriter) getProperties(i int, p *service.Message, batch service.MessageBatch) (map[string]any, error) {
	properties := make(map[string]any)
	if len(a.conf.Properties) == 0 {
		mBytes, err := p.AsBytes()
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(mBytes, &properties); err != nil {
			return nil, err
		}

		for property, v := range properties {
			switch v.(type) {
			case []any, map[string]any:
				m, err := json.Marshal(v)
				if err != nil {
					a.log.Errorf("error marshaling property: %v.", property)
				}
				properties[property] = string(m)
			}
		}
	} else {
		for property, value := range a.conf.Properties {
			var err error
			if properties[property], err = batch.TryInterpolatedString(i, value); err != nil {
				return nil, fmt.Errorf("property %v interpolation error: %w", property, err)
			}
		}
	}
	return properties, nil
}

func (a *azureTableStorageWriter) execBatch(ctx context.Context, writeReqs map[string]map[string]map[string][]*aztables.EDMEntity) error {
	for tn, pks := range writeReqs {
		table := a.conf.client.NewClient(tn)
		for _, tts := range pks {
			var err error
			for tt, entities := range tts {
				var batch []aztables.TransactionAction
				ne := len(entities)
				for i, entity := range entities {
					batch, err = a.addToBatch(batch, tt, entity)
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
							if _, err = table.CreateTable(ctx, nil); err != nil {
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

func (a *azureTableStorageWriter) addToBatch(batch []aztables.TransactionAction, transactionType string, entity *aztables.EDMEntity) ([]aztables.TransactionAction, error) {
	appendFunc := func(b []aztables.TransactionAction, t aztables.TransactionType, e *aztables.EDMEntity) ([]aztables.TransactionAction, error) {
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
	switch transactionType {
	case "INSERT":
		return appendFunc(batch, aztables.TransactionTypeAdd, entity)
	case "INSERT_MERGE":
		return appendFunc(batch, aztables.TransactionTypeInsertMerge, entity)
	case "INSERT_REPLACE":
		return appendFunc(batch, aztables.TransactionTypeInsertReplace, entity)
	case "UPDATE_MERGE":
		return appendFunc(batch, aztables.TransactionTypeUpdateMerge, entity)
	case "UPDATE_REPLACE":
		return appendFunc(batch, aztables.TransactionTypeUpdateReplace, entity)
	case "DELETE":
		return appendFunc(batch, aztables.TransactionTypeDelete, entity)
	default:
		return nil, errors.New("invalid transaction type")
	}
}

func (a *azureTableStorageWriter) Close(context.Context) error {
	return nil
}
