package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	bmongo "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMongoDB] = TypeSpec{
		constructor: fromSimpleConstructor(NewMongoDB),
		Summary:     `Inserts items into a MongoDB collection.`,
		Description: ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.MongoDB, conf.MongoDB.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: bmongo.ConfigDocs().Add(
			docs.FieldCommon(
				"operation",
				"The mongo operation to perform. Must be one of the following: insert-one, delete-one, delete-many, "+
					"replace-one, update-one.",
			),
			docs.FieldCommon(
				"write_concern",
				"The write concern settings for the mongo connection.",
			).WithChildren(bmongo.WriteConcernDocs()...),
			docs.FieldCommon(
				"document_map",
				"A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by " +
					"mapping the fields in the message to the mongodb fields. The document map is required for the operations " +
					"insert-one, replace-one and update-one.",
				bmongo.MapExamples(),
			),
			docs.FieldCommon(
				"filter_map",
				"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except " +
					"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should " +
					"have the fields required to locate the document to delete.",
				bmongo.MapExamples(),
			),
			docs.FieldCommon(
				"hint_map",
				"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations " +
					"except insert-one. It is used to improve performance of finding the documents in the mongodb.",
				bmongo.MapExamples(),
			),
			docs.FieldCommon(
				"max_in_flight",
				"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			batch.FieldSpec(),
		).Merge(retries.FieldSpecs()),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewMongoDB creates a new MongoDB output type.
func NewMongoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	return newMongoDB(TypeMongoDB, conf.MongoDB, mgr, log, stats)
}

func newMongoDB(name string, conf writer.MongoDBConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	m, err := writer.NewMongoDB(conf, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.MaxInFlight == 1 {
		w, err = NewWriter(name, m, log, stats)
	} else {
		w, err = NewAsyncWriter(name, conf.MaxInFlight, m, log, stats)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
