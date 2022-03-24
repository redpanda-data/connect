package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

func init() {
	Constructors[TypeAWSDynamoDB] = TypeSpec{
		constructor: fromSimpleConstructor(func(c Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
			return newDynamoDB(TypeAWSDynamoDB, c.AWSDynamoDB, mgr, log, stats)
		}),
		Version: "3.36.0",
		Summary: `
Inserts items into a DynamoDB table.`,
		Description: `
The field ` + "`string_columns`" + ` is a map of column names to string values,
where the values are
[function interpolated](/docs/configuration/interpolation#bloblang-queries) per message of a
batch. This allows you to populate string columns of an item by extracting
fields within the document payload or metadata like follows:

` + "```yml" + `
string_columns:
  id: ${!json("id")}
  title: ${!json("body.title")}
  topic: ${!meta("kafka_topic")}
  full_content: ${!content()}
` + "```" + `

The field ` + "`json_map_columns`" + ` is a map of column names to json paths,
where the [dot path](/docs/configuration/field_paths) is extracted from each document and
converted into a map value. Both an empty path and the path ` + "`.`" + ` are
interpreted as the root of the document. This allows you to populate map columns
of an item like follows:

` + "```yml" + `
json_map_columns:
  user: path.to.user
  whole_document: .
` + "```" + `

A column name can be empty:

` + "```yml" + `
json_map_columns:
  "": .
` + "```" + `

In which case the top level document fields will be written at the root of the
item, potentially overwriting previously defined column values. If a path is not
found within a document the column will not be populated.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`,
		Async:   true,
		Batches: true,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("table", "The table to store messages in."),
			docs.FieldString("string_columns", "A map of column keys to string values to store.",
				map[string]string{
					"id":           "${!json(\"id\")}",
					"title":        "${!json(\"body.title\")}",
					"topic":        "${!meta(\"kafka_topic\")}",
					"full_content": "${!content()}",
				},
			).IsInterpolated().Map(),
			docs.FieldString("json_map_columns", "A map of column keys to [field paths](/docs/configuration/field_paths) pointing to value data within messages.",
				map[string]string{
					"user":           "path.to.user",
					"whole_document": ".",
				},
				map[string]string{
					"": ".",
				},
			).Map(),
			docs.FieldString("ttl", "An optional TTL to set for items, calculated from the moment the message is sent.").Advanced(),
			docs.FieldString("ttl_key", "The column key to place the TTL value within.").Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			policy.FieldSpec(),
		).WithChildren(session.FieldSpecs()...).WithChildren(retries.FieldSpecs()...),
		Categories: []string{
			"Services",
			"AWS",
		},
	}
}

//------------------------------------------------------------------------------

func newDynamoDB(name string, conf writer.DynamoDBConfig, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	dyn, err := writer.NewDynamoDBV2(conf, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(name, conf.MaxInFlight, dyn, log, stats)
	if err != nil {
		return nil, err
	}
	return NewBatcherFromConfig(conf.Batching, w, mgr, log, stats)
}
