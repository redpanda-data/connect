// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/retries"
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
		Description(service.OutputPerformanceDocs(true, true)).
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(moFieldCollection).
				Description("The name of the target collection."),
			outputOperationDocs(OperationUpdateOne),
			writeConcernDocs(),
		).
		Fields(writeMapsFields()...).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(moFieldBatching),
		)
	for _, f := range retries.CommonRetryBackOffFields(3, "1s", "5s", "30s") {
		spec = spec.Field(f.Deprecated())
	}
	return spec
}

func init() {
	service.MustRegisterBatchOutput(
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
}

// ------------------------------------------------------------------------------

type outputWriter struct {
	log *service.Logger

	cc *ClientConfig
	// client and database are established lazily on Connect so that any
	// credentials are resolved fresh on every (re)connection attempt.
	client           *mongo.Client
	database         *mongo.Database
	collection       *service.InterpolatedString
	writeConcernSpec *writeConcernSpec
	operation        Operation
	writeMaps        writeMaps

	mu sync.Mutex
}

func newOutputWriter(conf *service.ParsedConfig, res *service.Resources) (db *outputWriter, err error) {
	db = &outputWriter{
		log: res.Logger(),
	}
	if db.cc, err = ClientConfigFromParsed(conf, res.Logger()); err != nil {
		return
	}
	if db.collection, err = conf.FieldInterpolatedString(moFieldCollection); err != nil {
		return
	}
	if db.writeConcernSpec, err = writeConcernSpecFromParsed(conf); err != nil {
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

// ConnectionTest attempts to test the connection configuration of this output
// without actually sending data. The connection, if successful, is then
// closed.
func (m *outputWriter) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	client, _, err := m.cc.Connect(ctx)
	if err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}
	defer func() {
		_ = client.Disconnect(ctx)
	}()
	if err := client.Ping(ctx, nil); err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("ping failed: %w", err)).AsList()
	}
	return service.ConnectionTestSucceeded().AsList()
}

// Connect attempts to establish a connection to the target mongo DB.
func (m *outputWriter) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil {
		// MongoDB never re-authenticates an established pool connection, so a
		// successful ping here only proves the old socket is still alive - it
		// does not prove the credential baked into the client's options is
		// still valid for a brand new socket. For role-derived (STS) session
		// credentials that snapshot can already be expired, so those clients
		// are always dropped and rebuilt below to re-run the credential
		// builder. Static keys and the ambient chain don't expire this way,
		// so the ping-reuse fast path stays for them.
		if !m.cc.AssumesRole() {
			if err := m.client.Ping(ctx, nil); err == nil {
				return nil
			}
		}
		_ = m.client.Disconnect(ctx)
		m.client = nil
	}

	client, database, err := m.cc.Connect(ctx)
	if err != nil {
		return err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return fmt.Errorf("ping failed: %v", err)
	}
	m.client, m.database = client, database
	return nil
}

func (m *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	m.mu.Lock()
	client, database, collection := m.client, m.database, m.collection
	m.mu.Unlock()

	if client == nil || collection == nil {
		return service.ErrNotConnected
	}

	writeModelsMap := map[string][]mongo.WriteModel{}
	wmExec := m.writeMaps.exec(batch)

	err := batch.WalkWithBatchedErrors(func(i int, _ *service.Message) error {
		var err error

		collectionStr, err := batch.TryInterpolatedString(i, collection)
		if err != nil {
			return fmt.Errorf("collection interpolation error: %w", err)
		}

		docJSON, filterJSON, hintJSON, err := wmExec.extractFromMessage(m.operation, i)
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
			if err := m.builkWrite(ctx, database, collectionStr, writeModels); err != nil {
				// Only errors raised by the driver itself can indicate a broken
				// pool. Reporting ErrNotConnected makes the framework re-run
				// Connect, which rebuilds the client and re-resolves any IAM
				// credentials, and retry this batch against it.
				if isConnPoolError(err) {
					m.log.Errorf("MongoDB connection failure, triggering reconnect: %v", err)
					return service.ErrNotConnected
				}
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

func (m *outputWriter) builkWrite(ctx context.Context, database *mongo.Database, collectionStr string, writeModels []mongo.WriteModel) error {
	if m.writeConcernSpec.wTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, m.writeConcernSpec.wTimeout)
		defer cancel()
	}

	// We should have at least one write model in the slice
	collection := database.Collection(collectionStr, m.writeConcernSpec.options)
	_, err := collection.BulkWrite(ctx, writeModels)
	return err
}

func (m *outputWriter) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	if m.client != nil {
		err = m.client.Disconnect(ctx)
		m.client = nil
	}
	m.database = nil
	m.collection = nil
	return err
}
