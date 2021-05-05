package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	ibatch "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	ioutput "github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func init() {
	bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c output.Config, nm bundle.NewManagement) (output.Type, error) {
		return NewOutput(c.MongoDB, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    output.TypeMongoDB,
		Type:    docs.TypeOutput,
		Status:  docs.StatusExperimental,
		Version: "3.43.0",
		Categories: []string{
			string(output.CategoryServices),
		},
		Summary:     `Inserts items into a MongoDB collection.`,
		Description: ioutput.Description(true, true, ""),
		Config: docs.FieldComponent().WithChildren(
			client.ConfigDocs().Add(
				docs.FieldCommon(
					"operation",
					"The mongo operation to perform. Must be one of the following: insert-one, delete-one, delete-many, "+
						"replace-one, update-one.",
				),
				docs.FieldCommon(
					"write_concern",
					"The write concern settings for the mongo connection.",
				).WithChildren(writeConcernDocs()...),
				docs.FieldCommon(
					"document_map",
					"A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by "+
						"mapping the fields in the message to the mongodb fields. The document map is required for the operations "+
						"insert-one, replace-one and update-one.",
					mapExamples()...,
				).Linter(docs.LintBloblangMapping),
				docs.FieldCommon(
					"filter_map",
					"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except "+
						"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should "+
						"have the fields required to locate the document to delete.",
					mapExamples()...,
				).Linter(docs.LintBloblangMapping),
				docs.FieldCommon(
					"hint_map",
					"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations "+
						"except insert-one. It is used to improve performance of finding the documents in the mongodb.",
					mapExamples()...,
				).Linter(docs.LintBloblangMapping),
				docs.FieldCommon(
					"max_in_flight",
					"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
				batch.FieldSpec(),
			).Merge(retries.FieldSpecs())...,
		),
	})
}

//------------------------------------------------------------------------------

// NewOutput creates a new MongoDB output type.
func NewOutput(conf output.MongoDBConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (output.Type, error) {
	m, err := NewWriter(conf, log, stats)
	if err != nil {
		return nil, err
	}
	var w output.Type
	if w, err = output.NewAsyncWriter(output.TypeMongoDB, conf.MaxInFlight, m, log, stats); err != nil {
		return w, err
	}
	return output.NewBatcherFromConfig(conf.Batching, w, mgr, log, stats)
}

// NewWriter creates a new MongoDB writer.Type.
func NewWriter(
	conf output.MongoDBConfig,
	log log.Modular,
	stats metrics.Type,
) (*Writer, error) {
	db := &Writer{
		conf:    conf,
		log:     log,
		stats:   stats,
		shutSig: shutdown.NewSignaller(),
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

	var filterNeeded, documentNeeded bool
	var hintAllowed bool

	if _, ok := documentMapOps[conf.Operation]; !ok {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one, or update-one", conf.Operation)
	}

	documentNeeded = documentMapOps[conf.Operation]
	filterNeeded = filterMapOps[conf.Operation]
	hintAllowed = hintAllowedOps[conf.Operation]

	var err error

	if filterNeeded {
		if conf.FilterMap == "" {
			return nil, errors.New("mongodb filter_map must be specified")
		}
		if db.filterMap, err = bloblang.NewMapping("", conf.FilterMap); err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", conf.Operation)
	}

	if documentNeeded {
		if conf.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}
		if db.documentMap, err = bloblang.NewMapping("", conf.DocumentMap); err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", conf.Operation)
	}

	if hintAllowed && conf.HintMap != "" {
		if db.hintMap, err = bloblang.NewMapping("", conf.HintMap); err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", conf.Operation)
	}

	if db.wcTimeout, err = time.ParseDuration(conf.WriteConcern.WTimeout); err != nil {
		return nil, fmt.Errorf("failed to parse write concern wtimeout string: %v", err)
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

	mu         sync.Mutex
	client     *mongo.Client
	collection *mongo.Collection

	shutSig *shutdown.Signaller
}

// ConnectWithContext attempts to establish a connection to the target mongo DB
func (m *Writer) ConnectWithContext(ctx context.Context) error {
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

	m.collection = client.
		Database(m.conf.MongoConfig.Database).
		Collection(m.conf.MongoConfig.Collection, options.Collection().SetWriteConcern(writeConcern))
	m.client = client
	return nil
}

// WriteWithContext attempts to perform the designated operation to the mongo DB collection.
func (m *Writer) WriteWithContext(ctx context.Context, msg types.Message) error {
	m.mu.Lock()
	collection := m.collection
	m.mu.Unlock()

	if collection == nil {
		return types.ErrNotConnected
	}

	var writeModels []mongo.WriteModel
	err := writer.IterateBatchedSend(msg, func(i int, _ types.Part) error {
		var err error
		var filterVal, documentVal types.Part
		var filterValWanted, documentValWanted bool

		filterValWanted = filterMapOps[m.conf.Operation]
		documentValWanted = documentMapOps[m.conf.Operation]

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

		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}
			if hintJSON, err = hintVal.JSON(); err != nil {
				return err
			}
		}

		var writeModel mongo.WriteModel
		upsertFalse := false
		switch m.conf.Operation {
		case "insert-one":
			writeModel = &mongo.InsertOneModel{
				Document: docJSON,
			}
		case "delete-one":
			writeModel = &mongo.DeleteOneModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case "delete-many":
			writeModel = &mongo.DeleteManyModel{
				Filter: filterJSON,
				Hint:   hintJSON,
			}
		case "replace-one":
			writeModel = &mongo.ReplaceOneModel{
				Upsert:      &upsertFalse,
				Filter:      filterJSON,
				Replacement: docJSON,
				Hint:        hintJSON,
			}
		case "update-one":
			writeModel = &mongo.UpdateOneModel{
				Upsert: &upsertFalse,
				Filter: filterJSON,
				Update: docJSON,
				Hint:   hintJSON,
			}
		}

		if writeModel != nil {
			writeModels = append(writeModels, writeModel)
		}
		return nil
	})

	var batchErr *ibatch.Error
	if err != nil {
		if !errors.As(err, &batchErr) {
			return err
		}
	}

	if len(writeModels) > 0 {
		if _, err = collection.BulkWrite(ctx, writeModels); err != nil {
			return err
		}
	}

	if batchErr != nil {
		return batchErr
	}
	return nil
}

// CloseAsync begins cleaning up resources used by this writer asynchronously.
func (m *Writer) CloseAsync() {
	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.client != nil {
			_ = m.client.Disconnect(context.Background())
			m.client = nil
		}
		m.collection = nil
		m.shutSig.ShutdownComplete()
	}()
}

// WaitForClose will block until either the writer is closed or a specified
// timeout occurs.
func (m *Writer) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
