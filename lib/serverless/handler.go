// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package serverless

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ServerlessResponseType is an output type that redirects pipeline outputs back
// to the handler.
const ServerlessResponseType = "serverless_response"

func init() {
	output.RegisterPlugin(
		ServerlessResponseType,
		func() interface{} {
			s := struct{}{}
			return &s
		},
		func(_ interface{}, _ types.Manager, logger log.Modular, stats metrics.Type) (types.Output, error) {
			return output.NewWriter(ServerlessResponseType, roundtrip.Writer{}, logger, stats)
		},
	)
	output.DocumentPlugin(ServerlessResponseType, "", func(conf interface{}) interface{} { return nil })
}

//------------------------------------------------------------------------------

// Handler contains a live Benthos pipeline and wraps it within an invoke
// handler.
type Handler struct {
	transactionChan chan types.Transaction
	done            func(exitTimeout time.Duration) error
}

// Close shuts down the underlying pipeline. If the shut down takes longer than
// the specified timeout it is aborted and an error is returned.
func (h *Handler) Close(tout time.Duration) error {
	return h.done(tout)
}

// Handle is a request/response func that injects a payload into the underlying
// Benthos pipeline and returns a result.
func (h *Handler) Handle(ctx context.Context, obj interface{}) (interface{}, error) {
	msg := message.New(nil)
	part := message.NewPart(nil)
	if err := part.SetJSON(obj); err != nil {
		return nil, err
	}
	msg.Append(part)

	store := roundtrip.NewResultStore()
	roundtrip.AddResultStore(msg, store)

	resChan := make(chan types.Response, 1)

	select {
	case h.transactionChan <- types.NewTransaction(msg, resChan):
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	select {
	case res := <-resChan:
		if res.Error() != nil {
			return nil, res.Error()
		}
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	resultBatches := store.Get()
	if len(resultBatches) == 0 {
		return map[string]interface{}{"message": "request successful"}, nil
	}

	lambdaResults := make([][]interface{}, len(resultBatches))
	for i, batch := range resultBatches {
		batchResults := make([]interface{}, batch.Len())
		if err := batch.Iter(func(j int, p types.Part) error {
			var merr error
			if batchResults[j], merr = p.JSON(); merr != nil {
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

	genBatchOfBatches := make([]interface{}, len(lambdaResults))
	for i, b := range lambdaResults {
		genBatchOfBatches[i] = b
	}
	return genBatchOfBatches, nil
}

// NewHandler returns a Handler by creating a Benthos pipeline.
func NewHandler(conf config.Type) (*Handler, error) {
	// Logging and stats aggregation.
	logger := log.New(os.Stdout, conf.Logger)

	// Create our metrics type.
	stats, err := metrics.New(conf.Metrics, metrics.OptSetLogger(logger))
	if err != nil {
		logger.Errorf("Failed to connect metrics aggregator: %v\n", err)
		stats = metrics.Noop()
	}

	// Create our tracer type.
	var trac tracer.Type
	if trac, err = tracer.New(conf.Tracer); err != nil {
		logger.Errorf("Failed to initialise tracer: %v\n", err)
		trac = tracer.Noop()
	}

	// Create resource manager.
	manager, err := manager.New(conf.Manager, types.NoopMgr(), logger, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %v", err)
	}

	// Create pipeline and output layers.
	var pipelineLayer types.Pipeline
	var outputLayer types.Output

	transactionChan := make(chan types.Transaction, 1)

	pipelineLayer, err = pipeline.New(
		conf.Pipeline, manager,
		logger.NewModule(".pipeline"), metrics.Namespaced(stats, "pipeline"),
	)
	if err == nil {
		outputLayer, err = output.New(
			conf.Output, manager,
			logger.NewModule(".output"), metrics.Namespaced(stats, "output"),
		)
	}
	if err == nil {
		err = pipelineLayer.Consume(transactionChan)
	}
	if err == nil {
		err = outputLayer.Consume(pipelineLayer.TransactionChan())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %v", err)
	}

	return &Handler{
		transactionChan: transactionChan,
		done: func(exitTimeout time.Duration) error {
			timesOut := time.Now().Add(exitTimeout)
			pipelineLayer.CloseAsync()
			outputLayer.CloseAsync()

			if err = outputLayer.WaitForClose(exitTimeout); err != nil {
				return fmt.Errorf("failed to cleanly close output layer: %v", err)
			}
			if err = pipelineLayer.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("failed to cleanly close pipeline layer: %v", err)
			}

			manager.CloseAsync()
			if err = manager.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("failed to cleanly close resources: %v", err)
			}

			trac.Close()

			if sCloseErr := stats.Close(); sCloseErr != nil {
				logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
			}
			return nil
		},
	}, nil
}

//------------------------------------------------------------------------------
