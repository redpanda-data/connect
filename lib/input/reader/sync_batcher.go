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

package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// SyncBatcher is a wrapper for reader.Sync implementations that applies a
// batching policy to incoming payloads. Once a batch is created and sent the
// next Acknowledge call applies to all messages of the prior batches.
type SyncBatcher struct {
	batcher *batch.Policy
	r       Sync
	ctx     context.Context
	close   func()
}

// NewSyncBatcher returns a new SyncBatcher wrapper around a reader.Async.
func NewSyncBatcher(
	batchConfig batch.PolicyConfig,
	r Sync,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*SyncBatcher, error) {
	policy, err := batch.NewPolicy(batchConfig, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
	if err != nil {
		return nil, fmt.Errorf("failed to construct batch policy: %v", err)
	}
	ctx, close := context.WithCancel(context.Background())
	return &SyncBatcher{
		batcher: policy,
		r:       r,
		ctx:     ctx,
		close:   close,
	}, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to the source, if unsuccessful
// returns an error. If the attempt is successful (or not necessary) returns
// nil.
func (p *SyncBatcher) Connect() error {
	return p.ConnectWithContext(p.ctx)
}

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *SyncBatcher) ConnectWithContext(ctx context.Context) error {
	return p.r.ConnectWithContext(ctx)
}

// Read attempts to read a new message from the source.
func (p *SyncBatcher) Read() (types.Message, error) {
	return p.ReadNextWithContext(p.ctx)
}

// ReadNextWithContext attempts to read a new message from the source.
func (p *SyncBatcher) ReadNextWithContext(ctx context.Context) (types.Message, error) {
	var forcedBatchDeadline time.Time
	if tout := p.batcher.UntilNext(); tout >= 0 {
		forcedBatchDeadline = time.Now().Add(tout)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, forcedBatchDeadline)
		defer cancel()
	}

	flushBatch := false
	for !flushBatch {
		msg, err := p.r.ReadNextWithContext(ctx)
		if err != nil {
			if !forcedBatchDeadline.IsZero() && !time.Now().Before(forcedBatchDeadline) {
				if batch := p.batcher.Flush(); batch != nil && batch.Len() > 0 {
					return batch, nil
				}
			}
			if err == types.ErrTimeout {
				// If the call "timed out" it could either mean that the context
				// was cancelled, in which case we want to return right now, or
				// that the underlying mechanism timed out, in which case we
				// simply want to try again.
				select {
				case <-ctx.Done():
					if batch := p.batcher.Flush(); batch != nil && batch.Len() > 0 {
						return batch, nil
					}
					return nil, types.ErrTimeout
				default:
				}
				continue
			}
			if err == types.ErrTypeClosed {
				if batch := p.batcher.Flush(); batch != nil && batch.Len() > 0 {
					return batch, nil
				}
			}
			return nil, err
		}

		msg.Iter(func(i int, part types.Part) error {
			flushBatch = p.batcher.Add(part) || flushBatch
			return nil
		})
	}

	msg := p.batcher.Flush()
	if msg == nil || msg.Len() == 0 {
		return nil, types.ErrTimeout
	}
	return msg, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (p *SyncBatcher) Acknowledge(err error) error {
	return p.AcknowledgeWithContext(context.Background(), err)
}

// AcknowledgeWithContext confirms whether or not our unacknowledged messages
// have been successfully propagated or not.
func (p *SyncBatcher) AcknowledgeWithContext(ctx context.Context, err error) error {
	return p.r.AcknowledgeWithContext(ctx, err)
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *SyncBatcher) CloseAsync() {
	p.close()
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *SyncBatcher) WaitForClose(tout time.Duration) error {
	return p.r.WaitForClose(tout)
}

//------------------------------------------------------------------------------
