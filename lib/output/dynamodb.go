package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDynamoDB] = TypeSpec{
		constructor: NewDynamoDB,
		Description: `
Inserts items into a DynamoDB table.

The field ` + "`string_columns`" + ` is a map of column names to string values,
where the values are
[function interpolated](/docs/configuration/interpolation#functions) per message of a
batch. This allows you to populate string columns of an item by extracting
fields within the document payload or metadata like follows:

` + "``` yaml" + `
string_columns:
  id: ${!json_field:id}
  title: ${!json_field:body.title}
  topic: ${!metadata:kafka_topic}
  full_content: ${!content}
` + "```" + `

The field ` + "`json_map_columns`" + ` is a map of column names to json paths,
where the [dot path](/docs/configuration/field_paths) is extracted from each document and
converted into a map value. Both an empty path and the path ` + "`.`" + ` are
interpreted as the root of the document. This allows you to populate map columns
of an item like follows:

` + "``` yaml" + `
json_map_columns:
  user: path.to.user
  whole_document: .
` + "```" + `

A column name can be empty:

` + "``` yaml" + `
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
[in this document](/docs/guides/aws).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.DynamoDB, conf.DynamoDB.Batching)
		},
		Async:   true,
		Batches: true,
	}
}

//------------------------------------------------------------------------------

// NewDynamoDB creates a new DynamoDB output type.
func NewDynamoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	dyn, err := writer.NewDynamoDB(conf.DynamoDB, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.DynamoDB.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeDynamoDB, dyn, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeDynamoDB, conf.DynamoDB.MaxInFlight, dyn, log, stats,
		)
	}
	if bconf := conf.DynamoDB.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
