package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/impl/mongodb/client"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

//------------------------------------------------------------------------------

func init() {
	bundle.AllProcessors.Add(func(c processor.Config, nm bundle.NewManagement) (processor.Type, error) {
		return NewProcessor(c, nm, nm.Logger(), nm.Metrics())
	}, docs.ComponentSpec{
		Name:    processor.TypeMongoDB,
		Type:    docs.TypeProcessor,
		Status:  docs.StatusExperimental,
		Version: "3.43.0",
		Categories: []string{
			string(processor.CategoryIntegration),
		},
		Summary: `Performs operations against MongoDB for each message, allowing you to store or retrieve data within message payloads.`,
		Config: docs.FieldComponent().WithChildren(
			client.ConfigDocs().Add(
				processorOperationDocs(client.OperationInsertOne),
				docs.FieldCommon("collection", "The name of the target collection in the MongoDB DB.").IsInterpolated(),
				docs.FieldCommon(
					"write_concern",
					"The write_concern settings for the mongo connection.",
				).WithChildren(writeConcernDocs()...),
				docs.FieldBloblang(
					"document_map",
					"A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by "+
						"mapping the fields in the message to the mongodb fields. The document map is required for the operations "+
						"insert-one, replace-one and update-one.",
					mapExamples()...,
				),
				docs.FieldBloblang(
					"filter_map",
					"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except "+
						"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should "+
						"have the fields required to locate the document to delete.",
					mapExamples()...,
				),
				docs.FieldBloblang(
					"hint_map",
					"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations "+
						"except insert-one. It is used to improve performance of finding the documents in the mongodb.",
					mapExamples()...,
				),
				docs.FieldCommon(
					"upsert",
					"The upsert setting is optional and only applies for update-one and replace-one operations. If the filter specified in filter_map matches,"+
						"the document is updated or replaced accordingly, otherwise it is created.",
				).HasDefault(false).HasType(docs.FieldTypeBool).AtVersion("3.60.0"),
				docs.FieldAdvanced(
					"json_marshal_mode",
					"The json_marshal_mode setting is optional and controls the format of the output message.",
				).HasDefault(client.JSONMarshalModeCanonical).HasType(docs.FieldTypeString).HasAnnotatedOptions(
					string(client.JSONMarshalModeCanonical), "A string format that emphasizes type preservation at the expense of readability and interoperability. "+
						"That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
					string(client.JSONMarshalModeRelaxed), "A string format that emphasizes readability and interoperability at the expense of type preservation."+
						"That is, conversion from relaxed format to BSON can lose type information.",
				).AtVersion("3.60.0"),
				processor.PartsFieldSpec,
			).Merge(retries.FieldSpecs())...,
		).ChildDefaultAndTypesFromStruct(processor.NewMongoDBConfig()),
	})
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from a mongo db for each message of a
// batch
type Processor struct {
	conf  processor.MongoDBConfig
	log   log.Modular
	stats metrics.Type

	client                       *mongo.Client
	collection                   *field.Expression
	database                     *mongo.Database
	writeConcernCollectionOption *options.CollectionOptions

	parts       []int
	filterMap   *mapping.Executor
	documentMap *mapping.Executor
	hintMap     *mapping.Executor
	operation   client.Operation

	shutSig *shutdown.Signaller

	mCount            metrics.StatCounter
	mErr              metrics.StatCounter
	mKeyAlreadyExists metrics.StatCounter
	mSent             metrics.StatCounter
	mBatchSent        metrics.StatCounter
}

// NewProcessor returns a MongoDB processor.
func NewProcessor(
	conf processor.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type,
) (types.Processor, error) {
	// TODO: Remove this after V4 lands and #972 is fixed
	operation := client.NewOperation(conf.MongoDB.Operation)
	if operation == client.OperationInvalid {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one, update-one or find-one", conf.MongoDB.Operation)
	}

	m := &Processor{
		conf:  conf.MongoDB,
		log:   log,
		stats: stats,

		parts:     conf.MongoDB.Parts,
		operation: operation,

		shutSig:           shutdown.NewSignaller(),
		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}

	if conf.MongoDB.MongoDB.URL == "" {
		return nil, errors.New("mongo url must be specified")
	}

	if conf.MongoDB.MongoDB.Database == "" {
		return nil, errors.New("mongo database must be specified")
	}

	if conf.MongoDB.MongoDB.Collection == "" {
		return nil, errors.New("mongo collection must be specified")
	}

	bEnv := mgr.BloblEnvironment()
	var err error

	if isFilterAllowed(m.operation) {
		if conf.MongoDB.FilterMap == "" {
			return nil, errors.New("mongodb filter_map must be specified")
		}
		if m.filterMap, err = bEnv.NewMapping(conf.MongoDB.FilterMap); err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.MongoDB.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if isDocumentAllowed(m.operation) {
		if conf.MongoDB.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}
		if m.documentMap, err = bEnv.NewMapping(conf.MongoDB.DocumentMap); err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.MongoDB.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if isHintAllowed(m.operation) && conf.MongoDB.HintMap != "" {
		if m.hintMap, err = bEnv.NewMapping(conf.MongoDB.HintMap); err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.MongoDB.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if !isUpsertAllowed(m.operation) && conf.MongoDB.Upsert {
		return nil, fmt.Errorf("mongodb upsert not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if m.client, err = conf.MongoDB.MongoDB.Client(); err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	if err = m.client.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	if err = m.client.Ping(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("ping failed: %v", err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(conf.MongoDB.WriteConcern.WTimeout); err != nil {
		return nil, fmt.Errorf("failed to parse wtimeout string: %v", err)
	}

	writeConcern := writeconcern.New(
		writeconcern.J(conf.MongoDB.WriteConcern.J),
		writeconcern.WTimeout(timeout))

	w, err := strconv.Atoi(conf.MongoDB.WriteConcern.W)
	if err != nil {
		writeconcern.WTagSet(conf.MongoDB.WriteConcern.W)
	} else {
		writeconcern.W(w)(writeConcern)
	}

	// This does some validation so we don't have to
	if _, _, err = writeConcern.MarshalBSONValue(); err != nil {
		return nil, fmt.Errorf("write_concern validation error: %w", err)
	}

	if m.collection, err = interop.NewBloblangField(mgr, m.conf.MongoDB.Collection); err != nil {
		return nil, fmt.Errorf("failed to parse collection expression: %v", err)
	}

	m.database = m.client.Database(conf.MongoDB.MongoDB.Database)
	m.writeConcernCollectionOption = options.Collection().SetWriteConcern(writeConcern)

	return m, nil
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *Processor) ProcessMessage(msg *message.Batch) ([]*message.Batch, types.Response) {
	m.mCount.Incr(1)
	newMsg := msg.Copy()

	writeModelsMap := map[*mongo.Collection][]mongo.WriteModel{}
	processor.IteratePartsWithSpanV2("mongodb", m.parts, newMsg, func(i int, s *tracing.Span, p *message.Part) error {
		var err error
		var filterVal, documentVal *message.Part
		var upsertVal, filterValWanted, documentValWanted bool

		filterValWanted = isFilterAllowed(m.operation)
		documentValWanted = isDocumentAllowed(m.operation)
		upsertVal = m.conf.Upsert

		if filterValWanted {
			if filterVal, err = m.filterMap.MapPart(i, msg); err != nil {
				return fmt.Errorf("failed to execute filter_map: %v", err)
			}
		}

		if (filterVal != nil || !filterValWanted) && documentValWanted {
			if documentVal, err = m.documentMap.MapPart(i, msg); err != nil {
				return fmt.Errorf("failed to execute document_map: %v", err)
			}
		}

		if filterVal == nil && filterValWanted {
			return fmt.Errorf("failed to generate filterVal")
		}

		if documentVal == nil && documentValWanted {
			return fmt.Errorf("failed to generate documentVal")
		}

		var docJSON, filterJSON, hintJSON interface{}

		if filterValWanted {
			if filterJSON, err = filterVal.JSON(); err != nil {
				return err
			}
		}

		if documentValWanted {
			if docJSON, err = documentVal.JSON(); err != nil {
				return err
			}
		}

		findOptions := &options.FindOneOptions{}
		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}
			if hintJSON, err = hintVal.JSON(); err != nil {
				return err
			}
			findOptions.Hint = hintJSON
		}

		var writeModel mongo.WriteModel
		collection := m.database.Collection(m.collection.String(i, msg), m.writeConcernCollectionOption)

		switch m.operation {
		case client.OperationInsertOne:
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case client.OperationDeleteOne:
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case client.OperationDeleteMany:
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case client.OperationReplaceOne:
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &upsertVal,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case client.OperationUpdateOne:
			writeModel = &mongo.UpdateOneModel{
				Upsert: &upsertVal,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		case client.OperationFindOne:
			var decoded interface{}
			err := collection.FindOne(context.Background(), filterJSON, findOptions).Decode(&decoded)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					return err
				}
				m.log.Errorf("Error decoding mongo db result, filter = %v: %s", filterJSON, err)
				return err
			}
			data, err := bson.MarshalExtJSON(decoded, m.conf.JSONMarshalMode == client.JSONMarshalModeCanonical, false)
			if err != nil {
				return err
			}

			p.Set(data)

			return nil
		}

		if writeModel != nil {
			writeModelsMap[collection] = append(writeModelsMap[collection], writeModel)
		}
		return nil
	})

	if len(writeModelsMap) > 0 {
		for collection, writeModels := range writeModelsMap {
			// We should have at least one write model in the slice
			if _, err := collection.BulkWrite(context.Background(), writeModels); err != nil {
				m.log.Errorf("Bulk write failed in mongodb processor: %v", err)
				for _, n := range m.parts {
					processor.FlagErr(newMsg.Get(n), err)
				}
			}
		}
	}

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]*message.Batch{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *Processor) CloseAsync() {
	go func() {
		m.client.Disconnect(context.Background())
		m.shutSig.ShutdownComplete()
	}()
}

// WaitForClose blocks until the processor has closed down.
func (m *Processor) WaitForClose(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return types.ErrTimeout
	case <-m.shutSig.HasClosedChan():
	}
	return nil
}

//------------------------------------------------------------------------------
