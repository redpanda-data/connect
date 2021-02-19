package writer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	bmongo "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	"github.com/Jeffail/benthos/v3/lib/bloblang"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/cenkalti/backoff/v4"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var documentMapOps = map[string]bool{
	"insert-one":  true,
	"delete-one":  false,
	"delete-many": false,
	"replace-one": true,
	"update-one":  true,
}

var filterMapOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
}

var hintAllowedOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
}

// MongoDBConfig contains config fields for the MongoDB output type.
type MongoDBConfig struct {
	MongoConfig bmongo.Config `json:",inline" yaml:",inline"`

	Operation    string              `json:"operation" yaml:"operation"`
	WriteConcern bmongo.WriteConcern `json:"write_concern" yaml:"write_concern"`

	FilterMap   string `json:"filter_map" yaml:"filter_map"`
	DocumentMap string `json:"document_map" yaml:"document_map"`
	HintMap     string `json:"hint_map" yaml:"hint_map"`

	//DeleteEmptyValue bool `json:"delete_empty_value" yaml:"delete_empty_value"`
	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	RetryConfig retries.Config     `json:",inline" yaml:",inline"`
	Batching    batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewMongoDBConfig creates a MongoDB populated with default values.
func NewMongoDBConfig() MongoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return MongoDBConfig{
		MongoConfig:  bmongo.NewConfig(),
		Operation:    "update-one",
		MaxInFlight:  1,
		RetryConfig:  rConf,
		Batching:     batch.NewPolicyConfig(),
		WriteConcern: bmongo.WriteConcern{},
	}
}

// NewMongoDB creates a new MongoDB writer.Type.
func NewMongoDB(
	conf MongoDBConfig,
	log log.Modular,
	stats metrics.Type,
) (*MongoDB, error) {
	db := &MongoDB{
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

// MongoDB is a benthos writer.Type implementation that writes messages to an
// MongoDB database.
type MongoDB struct {
	client     *mongo.Client
	collection *mongo.Collection
	conf       MongoDBConfig
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

var _ Type = (*MongoDB)(nil)

// Connect attempts to establish a connection to the target mongo DB
func (m *MongoDB) Connect() error {
	return m.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target mongo DB
func (m *MongoDB) ConnectWithContext(ctx context.Context) (err error) {
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

func (m *MongoDB) Write(msg types.Message) error {
	return m.WriteWithContext(context.Background(), msg)
}

func (m *MongoDB) close() {
	m.closeOnce.Do(func() {
		close(m.closingCh)
	})
}

// WriteWithContext attempts to perform the designated operation to the mongo DB collection.
func (m *MongoDB) WriteWithContext(ctx context.Context, msg types.Message) error {
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
func (m *MongoDB) CloseAsync() {
	m.close()
}

// WaitForClose will block until either the writer is closed or a specified
// timeout occurs.
func (m *MongoDB) WaitForClose(timeout time.Duration) error {
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
