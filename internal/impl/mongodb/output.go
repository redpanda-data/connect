package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	ibatch "github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/mongodb/client"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return NewOutput(c.MongoDB, nm)
	}), docs.ComponentSpec{
		Name:        "mongodb",
		Type:        docs.TypeOutput,
		Status:      docs.StatusExperimental,
		Version:     "3.43.0",
		Categories:  []string{"Services"},
		Summary:     `Inserts items into a MongoDB collection.`,
		Description: output.Description(true, true, ""),
		Config: docs.FieldComponent().WithChildren(
			client.ConfigDocs().Add(
				outputOperationDocs(client.OperationUpdateOne),
				docs.FieldString("collection", "The name of the target collection in the MongoDB DB.").IsInterpolated(),
				docs.FieldObject(
					"write_concern",
					"The write concern settings for the mongo connection.",
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
				docs.FieldInt(
					"max_in_flight",
					"The maximum number of parallel message batches to have in flight at any given time."),
				policy.FieldSpec(),
			).Merge(retries.FieldSpecs())...,
		).ChildDefaultAndTypesFromStruct(output.NewMongoDBConfig()),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// NewOutput creates a new MongoDB output type.
func NewOutput(conf output.MongoDBConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	m, err := NewWriter(mgr, conf, mgr.Logger(), mgr.Metrics())
	if err != nil {
		return nil, err
	}
	var w output.Streamed
	if w, err = output.NewAsyncWriter("mongodb", conf.MaxInFlight, m, mgr); err != nil {
		return w, err
	}
	return batcher.NewFromConfig(conf.Batching, w, mgr)
}

// NewWriter creates a new MongoDB writer.Type.
func NewWriter(
	mgr bundle.NewManagement,
	conf output.MongoDBConfig,
	log log.Modular,
	stats metrics.Type,
) (*Writer, error) {
	// TODO: Remove this after V4 lands and #972 is fixed
	operation := client.NewOperation(conf.Operation)
	if operation == client.OperationInvalid {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one or update-one", conf.Operation)
	}

	db := &Writer{
		conf:      conf,
		log:       log,
		stats:     stats,
		operation: operation,
	}

	if conf.MongoConfig.URL == "" {
		return nil, errors.New("mongo url must be specified")
	}

	if conf.MongoConfig.Database == "" {
		return nil, errors.New("mongo database must be specified")
	}

	if conf.MongoConfig.Collection == "" {
		return nil, errors.New("mongo collection must be specified")
	}

	bEnv := mgr.BloblEnvironment()
	var err error

	if isFilterAllowed(db.operation) {
		if conf.FilterMap == "" {
			return nil, errors.New("mongodb filter_map must be specified")
		}
		if db.filterMap, err = bEnv.NewMapping(conf.FilterMap); err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", db.operation)
	}

	if isDocumentAllowed(db.operation) {
		if conf.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}
		if db.documentMap, err = bEnv.NewMapping(conf.DocumentMap); err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", db.operation)
	}

	if isHintAllowed(db.operation) && conf.HintMap != "" {
		if db.hintMap, err = bEnv.NewMapping(conf.HintMap); err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", db.operation)
	}

	if !isUpsertAllowed(db.operation) && conf.Upsert {
		return nil, fmt.Errorf("mongodb upsert not allowed for '%s' operation", db.operation)
	}

	if len(conf.WriteConcern.WTimeout) > 0 {
		if db.wcTimeout, err = time.ParseDuration(conf.WriteConcern.WTimeout); err != nil {
			return nil, fmt.Errorf("failed to parse write concern wtimeout string: %v", err)
		}
	}
	if db.collection, err = mgr.BloblEnvironment().NewField(conf.MongoConfig.Collection); err != nil {
		return nil, fmt.Errorf("failed to parse collection expression: %v", err)
	}

	return db, nil
}

// Writer is a benthos writer.Type implementation that writes messages to an
// Writer database.
type Writer struct {
	conf  output.MongoDBConfig
	log   log.Modular
	stats metrics.Type

	wcTimeout time.Duration

	filterMap   *mapping.Executor
	documentMap *mapping.Executor
	hintMap     *mapping.Executor
	operation   client.Operation

	mu                           sync.Mutex
	client                       *mongo.Client
	collection                   *field.Expression
	database                     *mongo.Database
	writeConcernCollectionOption *options.CollectionOptions
}

// Connect attempts to establish a connection to the target mongo DB.
func (m *Writer) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil && m.collection != nil {
		return nil
	}
	if m.client != nil {
		_ = m.client.Disconnect(ctx)
	}

	client, err := m.conf.MongoConfig.Client()
	if err != nil {
		return fmt.Errorf("failed to create mongodb client: %v", err)
	}

	if err = client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err = client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping failed: %v", err)
	}

	writeConcern := writeconcern.New(
		writeconcern.J(m.conf.WriteConcern.J),
		writeconcern.WTimeout(m.wcTimeout))

	w, err := strconv.Atoi(m.conf.WriteConcern.W)
	if err != nil {
		writeconcern.WTagSet(m.conf.WriteConcern.W)
	} else {
		writeconcern.W(w)(writeConcern)
	}

	// This does some validation so we don't have to
	if _, _, err = writeConcern.MarshalBSONValue(); err != nil {
		_ = client.Disconnect(ctx)
		return fmt.Errorf("write_concern validation error: %w", err)
	}

	m.database = client.Database(m.conf.MongoConfig.Database)
	m.writeConcernCollectionOption = options.Collection().SetWriteConcern(writeConcern)

	m.client = client
	return nil
}

// WriteBatch attempts to perform the designated operation to the mongo DB collection.
func (m *Writer) WriteBatch(ctx context.Context, msg message.Batch) error {
	m.mu.Lock()
	collection := m.collection
	m.mu.Unlock()

	if collection == nil {
		return component.ErrNotConnected
	}

	writeModelsMap := map[*mongo.Collection][]mongo.WriteModel{}
	err := output.IterateBatchedSend(msg, func(i int, _ *message.Part) error {
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

		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}
			if hintJSON, err = hintVal.AsStructured(); err != nil {
				return err
			}
		}

		collectionStr, err := collection.String(i, msg)
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
		}

		if writeModel != nil {
			writeModelsMap[collection] = append(writeModelsMap[collection], writeModel)
		}
		return nil
	})

	// Check for fatal errors and exit immediately if we encounter one
	var batchErr *ibatch.Error
	if err != nil {
		if !errors.As(err, &batchErr) {
			return err
		}
	}

	// Dispatch any documents which IterateBatchedSend managed to process successfully
	if len(writeModelsMap) > 0 {
		for collection, writeModels := range writeModelsMap {
			// We should have at least one write model in the slice
			if _, err := collection.BulkWrite(context.Background(), writeModels); err != nil {
				return err
			}
		}
	}

	// Return any errors produced by invalid messages from the batch
	if batchErr != nil {
		return batchErr
	}
	return nil
}

// Close begins cleaning up resources used by this writer.
func (m *Writer) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	if m.client != nil {
		err = m.client.Disconnect(ctx)
		m.client = nil
	}
	m.collection = nil
	return err
}
