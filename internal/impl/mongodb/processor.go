package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

//------------------------------------------------------------------------------

func init() {
	err := bundle.AllProcessors.Add(func(c processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		v2Proc, err := NewProcessor(c, nm)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("", v2Proc, nm), nil
	}, docs.ComponentSpec{
		Name:       "mongodb",
		Type:       docs.TypeProcessor,
		Status:     docs.StatusExperimental,
		Version:    "3.43.0",
		Categories: []string{"Integration"},
		Summary:    `Performs operations against MongoDB for each message, allowing you to store or retrieve data within message payloads.`,
		Config: docs.FieldComponent().WithChildren(
			client.ConfigDocs().Add(
				processorOperationDocs(client.OperationInsertOne),
				docs.FieldString("collection", "The name of the target collection in the MongoDB DB.").IsInterpolated(),
				docs.FieldObject(
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
				docs.FieldBool(
					"upsert",
					"The upsert setting is optional and only applies for update-one and replace-one operations. If the filter specified in filter_map matches,"+
						"the document is updated or replaced accordingly, otherwise it is created.",
				).HasDefault(false).AtVersion("3.60.0"),
				docs.FieldString(
					"json_marshal_mode",
					"The json_marshal_mode setting is optional and controls the format of the output message.",
				).HasDefault(client.JSONMarshalModeCanonical).Advanced().HasAnnotatedOptions(
					string(client.JSONMarshalModeCanonical), "A string format that emphasizes type preservation at the expense of readability and interoperability. "+
						"That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
					string(client.JSONMarshalModeRelaxed), "A string format that emphasizes readability and interoperability at the expense of type preservation."+
						"That is, conversion from relaxed format to BSON can lose type information.",
				).AtVersion("3.60.0"),
			).Merge(retries.FieldSpecs())...,
		).ChildDefaultAndTypesFromStruct(processor.NewMongoDBConfig()),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from a mongo db for each message of a
// batch.
type Processor struct {
	conf   processor.MongoDBConfig
	log    log.Modular
	stats  metrics.Type
	tracer trace.TracerProvider

	client                       *mongo.Client
	collection                   *field.Expression
	database                     *mongo.Database
	writeConcernCollectionOption *options.CollectionOptions

	filterMap   *mapping.Executor
	documentMap *mapping.Executor
	hintMap     *mapping.Executor
	operation   client.Operation
}

// NewProcessor returns a MongoDB processor.
func NewProcessor(conf processor.Config, mgr bundle.NewManagement) (processor.V2Batched, error) {
	// TODO: V4 Remove this after V4 lands and #972 is fixed
	operation := client.NewOperation(conf.MongoDB.Operation)
	if operation == client.OperationInvalid {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one, update-one or find-one", conf.MongoDB.Operation)
	}

	m := &Processor{
		conf:   conf.MongoDB,
		log:    mgr.Logger(),
		stats:  mgr.Metrics(),
		tracer: mgr.Tracer(),

		operation: operation,
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
	if len(conf.MongoDB.WriteConcern.WTimeout) > 0 {
		if timeout, err = time.ParseDuration(conf.MongoDB.WriteConcern.WTimeout); err != nil {
			return nil, fmt.Errorf("failed to parse wtimeout string: %v", err)
		}
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

	if m.collection, err = mgr.BloblEnvironment().NewField(m.conf.MongoDB.Collection); err != nil {
		return nil, fmt.Errorf("failed to parse collection expression: %v", err)
	}

	m.database = m.client.Database(conf.MongoDB.MongoDB.Database)
	m.writeConcernCollectionOption = options.Collection().SetWriteConcern(writeConcern)

	return m, nil
}

// ProcessBatch applies the processor to a message batch, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *Processor) ProcessBatch(ctx context.Context, spans []*tracing.Span, batch message.Batch) ([]message.Batch, error) {
	writeModelsMap := map[*mongo.Collection][]mongo.WriteModel{}
	_ = batch.Iter(func(i int, p *message.Part) (err error) {
		defer func() {
			if err != nil {
				p.ErrorSet(err)
			}
		}()

		var filterVal, documentVal *message.Part
		var upsertVal, filterValWanted, documentValWanted bool

		filterValWanted = isFilterAllowed(m.operation)
		documentValWanted = isDocumentAllowed(m.operation)
		upsertVal = m.conf.Upsert

		if filterValWanted {
			if filterVal, err = m.filterMap.MapPart(i, batch); err != nil {
				return fmt.Errorf("failed to execute filter_map: %v", err)
			}
		}

		if (filterVal != nil || !filterValWanted) && documentValWanted {
			if documentVal, err = m.documentMap.MapPart(i, batch); err != nil {
				return fmt.Errorf("failed to execute document_map: %v", err)
			}
		}

		if filterVal == nil && filterValWanted {
			return fmt.Errorf("failed to generate filterVal")
		}

		if documentVal == nil && documentValWanted {
			return fmt.Errorf("failed to generate documentVal")
		}

		var docJSON, filterJSON, hintJSON any

		if filterValWanted {
			if filterJSON, err = filterVal.AsStructured(); err != nil {
				return err
			}
		}

		if documentValWanted {
			if docJSON, err = documentVal.AsStructured(); err != nil {
				return err
			}
		}

		findOptions := &options.FindOneOptions{}
		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, batch)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}
			if hintJSON, err = hintVal.AsStructured(); err != nil {
				return err
			}
			findOptions.Hint = hintJSON
		}

		collectionStr, err := m.collection.String(i, batch)
		if err != nil {
			return fmt.Errorf("collection interpolation error: %w", err)
		}

		var writeModel mongo.WriteModel
		collection := m.database.Collection(collectionStr, m.writeConcernCollectionOption)

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
			var decoded any
			err := collection.FindOne(context.Background(), filterJSON, findOptions).Decode(&decoded)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					return err
				}
				m.log.Errorf("Error decoding mongo db result, filter = %v: %s", filterJSON, err)
				return err
			}
			data, err := bson.MarshalExtJSON(decoded, m.conf.JSONMarshalMode == client.JSONMarshalModeCanonical, false)
			if err != nil {
				return err
			}

			p.SetBytes(data)

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
				_ = batch.Iter(func(i int, p *message.Part) error {
					processor.MarkErr(p, spans[i], err)
					return nil
				})
			}
		}
	}

	return []message.Batch{batch}, nil
}

// Close shuts down the processor and stops processing requests.
func (m *Processor) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
