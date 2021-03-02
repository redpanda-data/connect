package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/lib/bloblang"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func init() {
	bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c output.Config, nm bundle.NewManagement) (output.Type, error) {
		return NewOutput(c.MongoDB, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:   output.TypeMongoDB,
		Type:   docs.TypeOutput,
		Status: docs.StatusExperimental,
		Categories: []string{
			string(output.CategoryServices),
		},
		Summary: `Inserts items into a MongoDB collection.`,
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
					mapExamples(),
				),
				docs.FieldCommon(
					"filter_map",
					"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except "+
						"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should "+
						"have the fields required to locate the document to delete.",
					mapExamples(),
				),
				docs.FieldCommon(
					"hint_map",
					"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations "+
						"except insert-one. It is used to improve performance of finding the documents in the mongodb.",
					mapExamples(),
				),
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
		conf:  conf,
		log:   log,
		stats: stats,
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

		db.filterMap, err = bloblang.NewMapping(conf.FilterMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", conf.Operation)
	}

	if documentNeeded {
		if conf.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}

		db.documentMap, err = bloblang.NewMapping(conf.DocumentMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", conf.Operation)
	}

	if hintAllowed && conf.HintMap != "" {
		db.hintMap, err = bloblang.NewMapping(conf.HintMap)
		if err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", conf.Operation)
	}

	db.client, err = conf.MongoConfig.Client()
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %v", err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(conf.WriteConcern.WTimeout); err != nil {
		return nil, fmt.Errorf("failed to parse wtimeout string: %v", err)
	}

	writeConcern := writeconcern.New(
		writeconcern.J(conf.WriteConcern.J),
		writeconcern.WTimeout(timeout))

	w, err := strconv.Atoi(conf.WriteConcern.W)
	if err != nil {
		writeconcern.WTagSet(conf.WriteConcern.W)
	} else {
		writeconcern.W(w)(writeConcern)
	}

	// This does some validation so we don't have to
	if _, _, err = writeConcern.MarshalBSONValue(); err != nil {
		return nil, fmt.Errorf("write_concern validation error: %w", err)
	}

	db.collection = db.client.
		Database(conf.MongoConfig.Database).
		Collection(conf.MongoConfig.Collection, options.Collection().SetWriteConcern(writeConcern))

	if db.backoffCtor, err = conf.RetryConfig.GetCtor(); err != nil {
		return nil, err
	}

	db.boffPool = sync.Pool{
		New: func() interface{} {
			return db.backoffCtor()
		},
	}

	closedChan := make(chan struct{})
	db.closedChan = closedChan
	close(closedChan) // starts closed

	return db, nil
}

// Writer is a benthos writer.Type implementation that writes messages to an
// Writer database.
type Writer struct {
	client     *mongo.Client
	collection *mongo.Collection
	conf       output.MongoDBConfig
	log        log.Modular
	stats      metrics.Type

	filterMap   bloblang.Mapping
	documentMap bloblang.Mapping
	hintMap     bloblang.Mapping

	mu          sync.Mutex
	backoffCtor func() backoff.BackOff

	boffPool sync.Pool

	closedChan <-chan struct{}
	runningCh  chan struct{}
	closingCh  chan struct{}
	closeOnce  sync.Once
}

// Connect attempts to establish a connection to the target mongo DB
func (m *Writer) Connect() error {
	return m.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target mongo DB
func (m *Writer) ConnectWithContext(ctx context.Context) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client == nil {
		return errors.New("client is nil") // shouldn't happen
	}

	if m.collection == nil {
		return errors.New("collection is nil") // shouldn't happen
	}

	if m.closingCh != nil {
		select {
		case <-m.closingCh:
			return types.ErrTypeClosed
		default:
		}
	}

	if m.runningCh != nil {
		select {
		default:
			return nil // already connected
		case <-m.runningCh:
		}
	}

	m.runningCh = make(chan struct{})
	m.closingCh = make(chan struct{})

	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = m.client.Connect(ctxTimeout)
	if err != nil {
		close(m.runningCh)
		return fmt.Errorf("failed to connect: %v", err)
	}

	go func() {
		<-m.closingCh

		m.mu.Lock()
		defer m.mu.Unlock()

		defer close(m.runningCh)

		if err := m.client.Disconnect(context.Background()); err != nil {
			m.log.Warnf("error disconnecting: %v\n", err)
		}
	}()

	ctxTimeout, cancel = context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = m.client.Ping(ctxTimeout, nil)
	if err != nil {
		m.close() // clean up
		return fmt.Errorf("ping failed: %v", err)
	}

	return nil
}

func (m *Writer) Write(msg types.Message) error {
	return m.WriteWithContext(context.Background(), msg)
}

func (m *Writer) close() {
	m.closeOnce.Do(func() {
		close(m.closingCh)
	})
}

// WriteWithContext attempts to perform the designated operation to the mongo DB collection.
func (m *Writer) WriteWithContext(ctx context.Context, msg types.Message) error {
	if m.runningCh == nil {
		return types.ErrNotConnected
	}

	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	var writeModels []mongo.WriteModel

	iterErr := msg.Iter(func(i int, part types.Part) error {
		var err error
		var filterVal, documentVal types.Part
		var filterValWanted, documentValWanted bool

		filterValWanted = filterMapOps[m.conf.Operation]
		documentValWanted = documentMapOps[m.conf.Operation]

		if filterValWanted {
			filterVal, err = m.filterMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute filter_map: %v", err)
			}
		}

		// deleted()
		if (filterVal != nil || !filterValWanted) && documentValWanted {
			documentVal, err = m.documentMap.MapPart(i, msg)
			if err != nil {
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
			filterJSON, err = filterVal.JSON()
			if err != nil {
				return err
			}
		}

		if documentValWanted {
			docJSON, err = documentVal.JSON()
			if err != nil {
				return err
			}
		}

		if m.hintMap != nil {
			hintVal, err := m.hintMap.MapPart(i, msg)
			if err != nil {
				return fmt.Errorf("failed to execute hint_map: %v", err)
			}

			hintJSON, err = hintVal.JSON()
			if err != nil {
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

		if writeModel == nil {
			return nil
		}

		writeModels = append(writeModels, writeModel)

		return nil
	})
	if iterErr != nil {
		return iterErr
	}

	_, err := m.collection.BulkWrite(ctx, writeModels)
	if err != nil {
		return err
	}

	return nil
}

// CloseAsync begins cleaning up resources used by this writer asynchronously.
func (m *Writer) CloseAsync() {
	m.close()
}

// WaitForClose will block until either the writer is closed or a specified
// timeout occurs.
func (m *Writer) WaitForClose(timeout time.Duration) error {
	if m.runningCh == nil { // not running
		return nil
	}

	select {
	case <-m.runningCh:
		return nil
	case <-time.After(timeout):
		return types.ErrTimeout
	}
}
