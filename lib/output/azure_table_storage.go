package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAzureTableStorage] = TypeSpec{
		constructor: fromSimpleConstructor(NewAzureTableStorage),
		Status:      docs.StatusBeta,
		Version:     "3.36.0",
		Summary: `
Stores message parts in an Azure Table Storage table.`,
		Description: `
Only one authentication method is required, ` + "`storage_connection_string`" + ` or ` + "`storage_account` and `storage_access_key`" + `. If both are set then the ` + "`storage_connection_string`" + ` is given priority.

In order to set the ` + "`table_name`" + `,  ` + "`partition_key`" + ` and ` + "`row_key`" + `
you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are
calculated per message of a batch.

If the ` + "`properties`" + ` are not set in the config, all the ` + "`json`" + ` fields
are marshaled and stored in the table, which will be created if it does not exist.

The ` + "`object`" + ` and ` + "`array`" + ` fields are marshaled as strings. e.g.:

The JSON message:
` + "```json" + `
{
  "foo": 55,
  "bar": {
    "baz": "a",
    "bez": "b"
  },
  "diz": ["a", "b"]
}
` + "```" + `

Will store in the table the following properties:
` + "```yaml" + `
foo: '55'
bar: '{ "baz": "a", "bez": "b" }'
diz: '["a", "b"]'
` + "```" + `

It's also possible to use function interpolations to get or transform the properties values, e.g.:

` + "```yaml" + `
properties:
  device: '${! json("device") }'
  timestamp: '${! json("timestamp") }'
` + "```" + ``,
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"storage_account",
				"The storage account to upload messages to. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldCommon("table_name", "The table to store messages into.",
				`${!meta("kafka_topic")}`,
			).IsInterpolated(),
			docs.FieldCommon("partition_key", "The partition key.",
				`${!json("date")}`,
			).IsInterpolated(),
			docs.FieldCommon("row_key", "The row key.",
				`${!json("device")}-${!uuid_v4()}`,
			).IsInterpolated(),
			docs.FieldCommon("properties", "A map of properties to store into the table.").IsInterpolated().Map(),
			docs.FieldAdvanced("insert_type", "Type of insert operation").HasOptions(
				"INSERT", "INSERT_MERGE", "INSERT_REPLACE",
			).IsInterpolated(),
			docs.FieldCommon("max_in_flight",
				"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}

	Constructors[TypeTableStorage] = TypeSpec{
		constructor: fromSimpleConstructor(newDeprecatedTableStorage),
		Status:      docs.StatusDeprecated,
		Summary:     "This component has been renamed to [`azure_table_storage`](/docs/components/outputs/azure_table_storage).",
		Async:       true,
		Batches:     true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"storage_account",
				"The storage account to upload messages to. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldCommon(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldCommon("table_name", "The table to store messages into.",
				`${!meta("kafka_topic")}`,
			).IsInterpolated(),
			docs.FieldCommon("partition_key", "The partition key.",
				`${!json("date")}`,
			).IsInterpolated(),
			docs.FieldCommon("row_key", "The row key.",
				`${!json("device")}-${!uuid_v4()}`,
			).IsInterpolated(),
			docs.FieldCommon("properties", "A map of properties to store into the table.").IsInterpolated().Map(),
			docs.FieldAdvanced("insert_type", "Type of insert operation").HasOptions(
				"INSERT", "INSERT_MERGE", "INSERT_REPLACE",
			).IsInterpolated(),
			docs.FieldCommon("max_in_flight",
				"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("timeout", "The maximum period to wait on an upload before abandoning it and reattempting."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
			CategoryAzure,
		},
	}
}

//------------------------------------------------------------------------------

// NewAzureTableStorage creates a new NewAzureTableStorage output type.
func NewAzureTableStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	tableStorage, err := writer.NewAzureTableStorage(conf.AzureTableStorage, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeAzureTableStorage, conf.AzureTableStorage.MaxInFlight, tableStorage, log, stats,
	)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.AzureTableStorage.Batching, w, mgr, log, stats)
}

func newDeprecatedTableStorage(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	tableStorage, err := writer.NewAzureTableStorage(conf.TableStorage, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.TableStorage.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeTableStorage, tableStorage, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeTableStorage, conf.TableStorage.MaxInFlight, tableStorage, log, stats,
		)
	}
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.TableStorage.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
