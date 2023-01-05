package serverless

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

// ServerlessResponseType is an output type that redirects pipeline outputs back
// to the handler.
const ServerlessResponseType = "sync_response"

// Handler contains a live Benthos pipeline and wraps it within an invoke
// handler.
type Handler struct {
	transactionChan chan message.Transaction
	done            func(exitTimeout time.Duration) error
}

// Close shuts down the underlying pipeline. If the shut down takes longer than
// the specified timeout it is aborted and an error is returned.
func (h *Handler) Close(tout time.Duration) error {
	return h.done(tout)
}

// Handle is a request/response func that injects a payload into the underlying
// Benthos pipeline and returns a result.
func (h *Handler) Handle(ctx context.Context, obj any) (any, error) {
	part := message.NewPart(nil)
	part.SetStructuredMut(obj)
	msg := message.Batch{part}

	store := transaction.NewResultStore()
	transaction.AddResultStore(msg, store)

	resChan := make(chan error, 1)

	select {
	case h.transactionChan <- message.NewTransaction(msg, resChan):
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	select {
	case res := <-resChan:
		if res != nil {
			return nil, res
		}
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	resultBatches := store.Get()
	if len(resultBatches) == 0 {
		return map[string]any{"message": "request successful"}, nil
	}

	lambdaResults := make([][]any, len(resultBatches))
	for i, batch := range resultBatches {
		batchResults := make([]any, batch.Len())
		if err := batch.Iter(func(j int, p *message.Part) error {
			var merr error
			if batchResults[j], merr = p.AsStructured(); merr != nil {
				return fmt.Errorf("failed to marshal json response: %v", merr)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to process result batch '%v': %v", i, err)
		}
		lambdaResults[i] = batchResults
	}

	if len(lambdaResults) == 1 {
		if len(lambdaResults[0]) == 1 {
			return lambdaResults[0][0], nil
		}
		return lambdaResults[0], nil
	}

	genBatchOfBatches := make([]any, len(lambdaResults))
	for i, b := range lambdaResults {
		genBatchOfBatches[i] = b
	}
	return genBatchOfBatches, nil
}

// NewHandler returns a Handler by creating a Benthos pipeline.
func NewHandler(conf config.Type) (*Handler, error) {
	// Logging and stats aggregation.
	logger, err := log.NewV2(os.Stdout, conf.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %v", err)
	}

	// We use a temporary manager with just the logger initialised for metrics
	// instantiation. Doing this means that metrics plugins will use a global
	// environment for child plugins and bloblang mappings, which we might want
	// to revise in future.
	tmpMgr := mock.NewManager()
	tmpMgr.L = logger

	// Create our metrics type.
	var stats *metrics.Namespaced
	if stats, err = bundle.AllMetrics.Init(conf.Metrics, tmpMgr); err != nil {
		logger.Errorf("Failed to connect metrics aggregator: %v\n", err)
		stats = metrics.NewNamespaced(metrics.Noop())
	}

	// Create our tracer type.
	trac, err := bundle.AllTracers.Init(conf.Tracer, tmpMgr)
	if err != nil {
		logger.Errorf("Failed to initialise tracer: %v\n", err)
		trac = trace.NewNoopTracerProvider()
	}

	// Create resource manager.
	manager, err := manager.New(conf.ResourceConfig, manager.OptSetLogger(logger), manager.OptSetMetrics(stats), manager.OptSetTracer(trac))
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %v", err)
	}

	// Create pipeline and output layers.
	var pipelineLayer processor.Pipeline
	var outputLayer ioutput.Streamed

	transactionChan := make(chan message.Transaction, 1)

	pMgr := manager.IntoPath("pipeline")
	if pipelineLayer, err = pipeline.New(conf.Pipeline, pMgr); err != nil {
		return nil, fmt.Errorf("failed to create resource pipeline: %w", err)
	}

	oMgr := manager.IntoPath("output")
	if outputLayer, err = oMgr.NewOutput(conf.Output); err != nil {
		return nil, fmt.Errorf("failed to create resource output: %w", err)
	}

	if err = pipelineLayer.Consume(transactionChan); err != nil {
		return nil, fmt.Errorf("failed to create resource: %v", err)
	}

	if err = outputLayer.Consume(pipelineLayer.TransactionChan()); err != nil {
		return nil, fmt.Errorf("failed to create resource: %v", err)
	}

	return &Handler{
		transactionChan: transactionChan,
		done: func(exitTimeout time.Duration) error {
			close(transactionChan)

			ctx, done := context.WithTimeout(context.Background(), exitTimeout)
			defer done()

			outputLayer.TriggerCloseNow()
			if err = outputLayer.WaitForClose(ctx); err != nil {
				return fmt.Errorf("failed to cleanly close output layer: %v", err)
			}
			if err = pipelineLayer.WaitForClose(ctx); err != nil {
				return fmt.Errorf("failed to cleanly close pipeline layer: %v", err)
			}

			manager.TriggerStopConsuming()
			if err = manager.WaitForClose(ctx); err != nil {
				return fmt.Errorf("failed to cleanly close resources: %v", err)
			}

			defer func() {
				if shutter, ok := trac.(interface {
					Shutdown(context.Context) error
				}); ok {
					_ = shutter.Shutdown(context.Background())
				}
			}()

			if sCloseErr := stats.Close(); sCloseErr != nil {
				logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
			}
			return nil
		},
	}, nil
}

//------------------------------------------------------------------------------
