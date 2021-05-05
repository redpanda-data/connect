package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/opentracing/opentracing-go"
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
	"find-one":    false,
}

var filterMapOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
	"find-one":    true,
}

var hintAllowedOps = map[string]bool{
	"insert-one":  false,
	"delete-one":  true,
	"delete-many": true,
	"replace-one": true,
	"update-one":  true,
	"find-one":    true,
}

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
				docs.FieldCommon(
					"operation",
					"The mongodb operation to perform. Must be one of the following: insert-one, delete-one, delete-many, "+
						"replace-one, update-one, find-one.",
				),
				docs.FieldCommon(
					"write_concern",
					"The write_concern settings for the mongo connection.",
				).WithChildren(writeConcernDocs()...),
				docs.FieldCommon(
					"document_map",
					"A bloblang map representing the records in the mongo db. Used to generate the document for mongodb by "+
						"mapping the fields in the message to the mongodb fields. The document map is required for the operations "+
						"insert-one, replace-one and update-one.",
					mapExamples(),
				).Linter(docs.LintBloblangMapping),
				docs.FieldCommon(
					"filter_map",
					"A bloblang map representing the filter for the mongo db command. The filter map is required for all operations except "+
						"insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should "+
						"have the fields required to locate the document to delete.",
					mapExamples(),
				).Linter(docs.LintBloblangMapping),
				docs.FieldCommon(
					"hint_map",
					"A bloblang map representing the hint for the mongo db command. This map is optional and is used with all operations "+
						"except insert-one. It is used to improve performance of finding the documents in the mongodb.",
					mapExamples(),
				).Linter(docs.LintBloblangMapping),
				processor.PartsFieldSpec,
			).Merge(retries.FieldSpecs())...,
		),
	})
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from a mongo db for each message of a
// batch
type Processor struct {
	conf  processor.MongoDBConfig
	log   log.Modular
	stats metrics.Type

	client     *mongo.Client
	collection *mongo.Collection

	parts       []int
	filterMap   *mapping.Executor
	documentMap *mapping.Executor
	hintMap     *mapping.Executor

	shutSig *shutdown.Signaller

	mCount            metrics.StatCounter
	mErr              metrics.StatCounter
	mKeyAlreadyExists metrics.StatCounter
	mSent             metrics.StatCounter
	mBatchSent        metrics.StatCounter
}

// NewProcessor returns a MongoDB processor.
func NewProcessor(
	conf processor.Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (types.Processor, error) {
	m := &Processor{
		conf:  conf.MongoDB,
		log:   log,
		stats: stats,

		parts: conf.MongoDB.Parts,

		shutSig:           shutdown.NewSignaller(),
		mCount:            stats.GetCounter("count"),
		mErr:              stats.GetCounter("error"),
		mKeyAlreadyExists: stats.GetCounter("key_already_exists"),
		mSent:             stats.GetCounter("sent"),
		mBatchSent:        stats.GetCounter("batch.sent"),
	}

	if conf.MongoDB.Operation == "" {
		return nil, fmt.Errorf("operator is a required field")
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

	var filterNeeded, documentNeeded bool
	var hintAllowed bool

	if _, ok := documentMapOps[conf.MongoDB.Operation]; !ok {
		return nil, fmt.Errorf("mongodb operation '%s' unknown: must be insert-one, delete-one, delete-many, replace-one, update-one or find-one", conf.MongoDB.Operation)
	}

	documentNeeded = documentMapOps[conf.MongoDB.Operation]
	filterNeeded = filterMapOps[conf.MongoDB.Operation]
	hintAllowed = hintAllowedOps[conf.MongoDB.Operation]

	var err error

	if filterNeeded {
		if conf.MongoDB.FilterMap == "" {
			return nil, errors.New("mongodb filter_map must be specified")
		}
		if m.filterMap, err = bloblang.NewMapping("", conf.MongoDB.FilterMap); err != nil {
			return nil, fmt.Errorf("failed to parse filter_map: %v", err)
		}
	} else if conf.MongoDB.FilterMap != "" {
		return nil, fmt.Errorf("mongodb filter_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if documentNeeded {
		if conf.MongoDB.DocumentMap == "" {
			return nil, errors.New("mongodb document_map must be specified")
		}
		if m.documentMap, err = bloblang.NewMapping("", conf.MongoDB.DocumentMap); err != nil {
			return nil, fmt.Errorf("failed to parse document_map: %v", err)
		}
	} else if conf.MongoDB.DocumentMap != "" {
		return nil, fmt.Errorf("mongodb document_map not allowed for '%s' operation", conf.MongoDB.Operation)
	}

	if hintAllowed && conf.MongoDB.HintMap != "" {
		if m.hintMap, err = bloblang.NewMapping("", conf.MongoDB.HintMap); err != nil {
			return nil, fmt.Errorf("failed to parse hint_map: %v", err)
		}
	} else if conf.MongoDB.HintMap != "" {
		return nil, fmt.Errorf("mongodb hint_map not allowed for '%s' operation", conf.MongoDB.Operation)
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

	m.collection = m.client.
		Database(conf.MongoDB.MongoDB.Database).
		Collection(conf.MongoDB.MongoDB.Collection, options.Collection().SetWriteConcern(writeConcern))

	return m, nil
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (m *Processor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.mCount.Incr(1)
	newMsg := msg.Copy()

	var writeModels []mongo.WriteModel
	processor.IteratePartsWithSpan("mongodb", m.parts, newMsg, func(i int, s opentracing.Span, p types.Part) error {
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
		case "find-one":
			result := m.collection.FindOne(context.Background(), filterJSON, findOptions)
			bsonValue, err := result.DecodeBytes()
			if err != nil {
				m.log.Errorf("Error finding document in mongo db, filter = %v", filterJSON)
				return err
			}

			data := map[string]interface{}{}
			elements, err := bsonValue.Elements()
			if err != nil {
				m.log.Errorf("Error getting elements from document in mongo db, filter = %v", filterJSON)
				return err
			}

			for _, e := range elements {
				key := e.Key()
				value := e.Value().String()
				data[key] = value
			}
			return p.SetJSON(data)
		}

		if writeModel != nil {
			writeModels = append(writeModels, writeModel)
		}
		return nil
	})

	if len(writeModels) > 0 {
		if _, err := m.collection.BulkWrite(context.Background(), writeModels); err != nil {
			m.log.Errorf("Bulk write failed in mongodb processor: %v", err)
			for _, n := range m.parts {
				processor.FlagErr(newMsg.Get(n), err)
			}
		}
	}

	m.mBatchSent.Incr(1)
	m.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
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
