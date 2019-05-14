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

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/Jeffail/benthos/lib/config"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/manager"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/tracer"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/aws/aws-lambda-go/lambda"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var transactionChan chan types.Transaction
var closeFn func()

// outputTransactionChan is the transaction channel extracted from the
// processing pipeline when we're in sync mode.
var outputTransactionChan <-chan types.Transaction

var requestHandler func(ctx context.Context, msg types.Message) (interface{}, error)

func init() {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}

	conf := config.New()

	if confStr := os.Getenv("BENTHOS_CONFIG"); len(confStr) > 0 {
		confBytes := text.ReplaceEnvVariables([]byte(confStr))
		if err := yaml.Unmarshal(confBytes, &conf); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := os.Stat(path); err == nil {
				if _, err = config.Read(path, true, &conf); err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
					os.Exit(1)
				}
				break
			}
		}
	}

	// Logging and stats aggregation.
	logger := log.New(os.Stdout, conf.Logger)

	// Create our metrics type.
	stats, err := metrics.New(conf.Metrics, metrics.OptSetLogger(logger))
	for err != nil {
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
		logger.Errorf("Failed to create resource: %v\n", err)
		os.Exit(1)
	}

	// Create pipeline and output layers.
	var pipelineLayer types.Pipeline
	var outputLayer types.Output

	transactionChan = make(chan types.Transaction, 1)

	pipelineLayer, err = pipeline.New(
		conf.Pipeline, manager,
		logger.NewModule(".pipeline"), metrics.Namespaced(stats, "pipeline"),
	)
	if err == nil {
		if conf.Output.Type != output.TypeSTDOUT {
			outputLayer, err = output.New(
				conf.Output, manager,
				logger.NewModule(".output"), metrics.Namespaced(stats, "output"),
			)
			requestHandler = handleAsyncRequest
		} else {
			requestHandler = handleSyncRequest
		}
	}
	if err == nil {
		err = pipelineLayer.Consume(transactionChan)
	}
	if err == nil {
		if outputLayer == nil {
			outputTransactionChan = pipelineLayer.TransactionChan()
		} else {
			err = outputLayer.Consume(pipelineLayer.TransactionChan())
		}
	}
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		os.Exit(1)
	}

	closeFn = func() {
		exitTimeout := time.Second * 30
		timesOut := time.Now().Add(exitTimeout)
		pipelineLayer.CloseAsync()
		if outputLayer != nil {
			outputLayer.CloseAsync()
			if err = outputLayer.WaitForClose(exitTimeout); err != nil {
				os.Exit(1)
			}
		}
		if err = pipelineLayer.WaitForClose(time.Until(timesOut)); err != nil {
			os.Exit(1)
		}

		manager.CloseAsync()
		if err = manager.WaitForClose(time.Until(timesOut)); err != nil {
			logger.Warnf(
				"Service failed to close cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr.\n", err,
			)
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}

		trac.Close()

		if sCloseErr := stats.Close(); sCloseErr != nil {
			logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
		}
	}
}

//------------------------------------------------------------------------------

func handleAsyncRequest(ctx context.Context, msg types.Message) (interface{}, error) {
	resChan := make(chan types.Response, 1)

	select {
	case transactionChan <- types.NewTransaction(msg, resChan):
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

	return map[string]interface{}{"message": "request successful"}, nil
}

func handleSyncRequest(ctx context.Context, msg types.Message) (interface{}, error) {
	resChan := make(chan types.Response, 1)

	select {
	case transactionChan <- types.NewTransaction(msg, resChan):
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	var tResult types.Transaction
	select {
	case res := <-resChan:
		if res.Error() != nil {
			return nil, res.Error()
		}
		return nil, errors.New("unexpected pipeline response")
	case tResult = <-outputTransactionChan:
		go func() {
			tResult.ResponseChan <- response.NewAck()
		}()
	case <-ctx.Done():
		return nil, errors.New("request cancelled")
	}

	if tResult.Payload.Len() == 1 {
		jResult, err := tResult.Payload.Get(0).JSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json response: %v", err)
		}
		return jResult, nil
	}

	var results []interface{}
	if err := tResult.Payload.Iter(func(i int, p types.Part) error {
		jResult, err := p.JSON()
		if err != nil {
			return fmt.Errorf("failed to marshal json response: %v", err)
		}
		results = append(results, jResult)
		return nil
	}); err != nil {
		return nil, err
	}
	return results, nil
}

func handleRequest(ctx context.Context, obj interface{}) (interface{}, error) {
	msg := message.New(nil)
	part := message.NewPart(nil)
	if err := part.SetJSON(obj); err != nil {
		return nil, err
	}
	msg.Append(part)

	return requestHandler(ctx, msg)
}

func main() {
	lambda.Start(handleRequest)
	closeFn()
}

//------------------------------------------------------------------------------
