// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncroutine

import (
	"context"
	"fmt"
)

type (
	// Batcher is a object for managing a background goroutine that accepts a number of requests
	// and executes them serially.
	Batcher[Request, Response any] struct {
		requestChan chan batcherRequest[Request, Response]

		cancel context.CancelFunc
		done   chan any
	}
	batcherRequest[Request, Response any] struct {
		req    Request
		respCh chan batcherResponse[Response]
	}
	batcherResponse[Response any] struct {
		resp Response
		err  error
	}
)

// NewBatcher creates a background goroutine that collects batches of requests and calls `fn`
// with them. `fn` should take a number of requests and return a number of responses, where the
// index of each request should line up the resulting response slice if error is `nil`.
func NewBatcher[Request, Response any](
	maxBatchSize int,
	fn func(context.Context, []Request) ([]Response, error),
) (*Batcher[Request, Response], error) {
	if maxBatchSize <= 0 {
		return nil, fmt.Errorf("invalid maxBatchSize=%d, must be > 0", maxBatchSize)
	}
	b := &Batcher[Request, Response]{
		requestChan: make(chan batcherRequest[Request, Response], maxBatchSize),
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.done = make(chan any)
	go b.runLoop(ctx, fn)
	return b, nil
}

func (b *Batcher[Request, Response]) runLoop(ctx context.Context, fn func(context.Context, []Request) ([]Response, error)) {
	defer func() {
		close(b.done)
	}()
	for {
		batch := b.dequeueAll(ctx)
		if len(batch) == 0 {
			return
		}
		batchRequest := make([]Request, len(batch))
		for i, msg := range batch {
			batchRequest[i] = msg.req
		}
		responses, err := fn(ctx, batchRequest)
		if err == nil && len(responses) != len(batch) {
			err = fmt.Errorf("invalid number of responses, expected=%d got=%d", len(batch), len(responses))
		}
		if err != nil {
			for _, msg := range batch {
				msg.respCh <- batcherResponse[Response]{err: err}
			}
			continue
		}
		for i, resp := range responses {
			batch[i].respCh <- batcherResponse[Response]{resp: resp}
		}
	}
}

func (b *Batcher[Request, Response]) dequeueAll(ctx context.Context) (batch []batcherRequest[Request, Response]) {
	for {
		if len(batch) >= cap(b.requestChan) {
			return
		}
		select {
		case req := <-b.requestChan:
			batch = append(batch, req)
		default:
			if len(batch) > 0 {
				return
			}
			// Blocking wait for next request
			select {
			case req := <-b.requestChan:
				batch = append(batch, req)
				// look and see if another request snuck in, otherwise we'll exit next iteration of the loop.
			case <-ctx.Done():
				return
			}
		}
	}
}

// Submit sends a request to be batched with other requests, the response and error is returned.
func (b *Batcher[Request, Response]) Submit(ctx context.Context, req Request) (resp Response, err error) {
	respCh := make(chan batcherResponse[Response], 1)
	b.requestChan <- batcherRequest[Request, Response]{req, respCh}
	select {
	case br := <-respCh:
		resp = br.resp
		err = br.err
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

// Close cancels any outgoing requests and waits for the background goroutine to exit.
//
// NOTE: One should *never* call Submit after calling Close (even if Close hasn't returned yet).
func (b *Batcher[Request, Response]) Close() {
	if b.cancel == nil {
		return
	}
	b.cancel()
	<-b.done
	b.done = nil
	b.cancel = nil
	close(b.requestChan)
	for req := range b.requestChan {
		req.respCh <- batcherResponse[Response]{err: context.Canceled}
	}
}
