package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	moFieldCollection = "collection"
	moFieldBatching   = "batching"
	moFieldRetries    = "retries"
)

func outputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("3.43.0").
		Categories("Services").
		Summary("Inserts items into a MongoDB collection.").
		Description(output.Description(true, true, "")).
		Fields(clientFields()...).
		Fields(
			service.NewStringField(moFieldCollection).
				Description("The name of the target collection."),
			service.NewInternalField(outputOperationDocs(OperationUpdateOne)),
			service.NewInternalField(writeConcernDocs()),
		).
		Fields(writeMapsFields()...).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(moFieldBatching),
		)
	for _, f := range pure.CommonRetryBackOffFields(3, "1s", "5s", "30s") {
		spec = spec.Field(f.Deprecated())
	}
	return spec
}

func init() {
	err := service.RegisterBatchOutput(
		"mongodb", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(moFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newOutputWriter(conf, mgr); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------

type outputWriter struct {
	log *service.Logger

	client                       *mongo.Client
	database                     *mongo.Database
	collection                   *service.InterpolatedString
	writeConcernCollectionOption *options.CollectionOptions
	operation                    Operation
	writeMaps                    writeMaps

	mu sync.Mutex
}

func newOutputWriter(conf *service.ParsedConfig, res *service.Resources) (db *outputWriter, err error) {
	db = &outputWriter{
		log: res.Logger(),
	}
	if db.client, db.database, err = getClient(conf); err != nil {
		return
	}
	if db.collection, err = conf.FieldInterpolatedString(moFieldCollection); err != nil {
		return
	}
	if db.writeConcernCollectionOption, err = writeConcernCollectionOptionFromParsed(conf); err != nil {
		return
	}
	if db.operation, err = operationFromParsed(conf); err != nil {
		return
	}
	if db.writeMaps, err = writeMapsFromParsed(conf, db.operation); err != nil {
		return
	}
	return db, nil
}

// Connect attempts to establish a connection to the target mongo DB.
func (m *outputWriter) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.client.Ping(ctx, nil); err != nil {
		_ = m.client.Disconnect(ctx)
		return fmt.Errorf("ping failed: %v", err)
	}
	return nil
}

func (m *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	m.mu.Lock()
	collection := m.collection
	m.mu.Unlock()

	if collection == nil {
		return service.ErrNotConnected
	}

	writeModelsMap := map[string][]mongo.WriteModel{}

	err := batch.WalkWithBatchedErrors(func(i int, _ *service.Message) error {
		var err error

		collectionStr, err := batch.TryInterpolatedString(i, collection)
		if err != nil {
			return fmt.Errorf("collection interpolation error: %w", err)
		}

		docJSON, filterJSON, hintJSON, err := m.writeMaps.extractFromMessage(m.operation, i, batch)
		if err != nil {
			return err
		}

		var writeModel mongo.WriteModel
		switch m.operation {
		case OperationInsertOne:
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case OperationDeleteOne:
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case OperationDeleteMany:
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case OperationReplaceOne:
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &m.writeMaps.upsert,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case OperationUpdateOne:
			writeModel = &mongo.UpdateOneModel{
				Upsert: &m.writeMaps.upsert,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		case OperationUpdateMany:
			writeModel = &mongo.UpdateManyModel{
				Upsert: &m.writeMaps.upsert,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		}

		if writeModel != nil {
			writeModelsMap[collectionStr] = append(writeModelsMap[collectionStr], writeModel)
		}
		return nil
	})

	// Check for fatal errors and exit immediately if we encounter one
	var batchErr *service.BatchError
	if err != nil {
		if !errors.As(err, &batchErr) {
			return err
		}
	}

	// Dispatch any documents which WalkWithBatchedErrors managed to process successfully
	if len(writeModelsMap) > 0 {
		for collectionStr, writeModels := range writeModelsMap {
			// We should have at least one write model in the slice
			collection := m.database.Collection(collectionStr, m.writeConcernCollectionOption)
			if _, err := collection.BulkWrite(ctx, writeModels); err != nil {
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

func (m *outputWriter) Close(ctx context.Context) error {
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
