package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// AsyncBatcher is a wrapper for reader.Async implementations that applies a
// batching policy to incoming payloads. Once a batch is created and sent the
// provided ack function ensures all messages within the batch are acked.
type AsyncBatcher struct {
	pendingAcks []AsyncAckFn
	batcher     *batch.Policy
	r           Async
}

// NewAsyncBatcher returns a new AsyncBatcher wrapper around a reader.Async.
func NewAsyncBatcher(
	batchConfig batch.PolicyConfig,
	r Async,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Async, error) {
	if batchConfig.IsNoop() {
		return r, nil
	}
	mgr, log, stats = interop.LabelChild("batching", mgr, log, stats)
	policy, err := batch.NewPolicy(batchConfig, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("failed to construct batch policy: %v", err)
	}
	return &AsyncBatcher{
		batcher: policy,
		r:       r,
	}, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to the source, if
// unsuccessful returns an error. If the attempt is successful (or not
// necessary) returns nil.
func (p *AsyncBatcher) ConnectWithContext(ctx context.Context) error {
	return p.r.ConnectWithContext(ctx)
}

func (p *AsyncBatcher) wrapAckFns() AsyncAckFn {
	ackFns := p.pendingAcks
	p.pendingAcks = nil
	return func(ctx context.Context, res types.Response) error {
		errs := []error{}
		errChan := make(chan error)
		for _, fn := range ackFns {
			go func(f AsyncAckFn) {
				errChan <- f(ctx, res)
			}(fn)
		}
		for range ackFns {
			if err := <-errChan; err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) == 0 {
			return nil
		}
		if len(errs) == 1 {
			return errs[0]
		}
		return fmt.Errorf("multiple messages failed to ack: %v", errs)
	}
}

// ReadWithContext attempts to read a new message from the source.
func (p *AsyncBatcher) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var forcedBatchDeadline time.Time
	if tout := p.batcher.UntilNext(); tout >= 0 {
		forcedBatchDeadline = time.Now().Add(tout)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, forcedBatchDeadline)
		defer cancel()
	}

	flushBatch := false
	for !flushBatch {
		msg, ackFn, err := p.r.ReadWithContext(ctx)
		if err != nil {
			if !forcedBatchDeadline.IsZero() && !time.Now().Before(forcedBatchDeadline) {
				if batch := p.batcher.Flush(); batch != nil && batch.Len() > 0 {
					return batch, p.wrapAckFns(), nil
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
						return batch, p.wrapAckFns(), nil
					}
					return nil, nil, types.ErrTimeout
				default:
				}
				continue
			}
			if err == types.ErrTypeClosed {
				if batch := p.batcher.Flush(); batch != nil && batch.Len() > 0 {
					return batch, p.wrapAckFns(), nil
				}
			}
			return nil, nil, err
		}

		p.pendingAcks = append(p.pendingAcks, ackFn)
		msg.Iter(func(i int, part types.Part) error {
			flushBatch = p.batcher.Add(part) || flushBatch
			return nil
		})
	}
	return p.batcher.Flush(), p.wrapAckFns(), nil
}

// CloseAsync triggers the asynchronous closing of the reader.
func (p *AsyncBatcher) CloseAsync() {
	p.r.CloseAsync()
}

// WaitForClose blocks until either the reader is finished closing or a timeout
// occurs.
func (p *AsyncBatcher) WaitForClose(tout time.Duration) error {
	tstop := time.Now().Add(tout)
	err := p.r.WaitForClose(time.Until(tstop))
	p.batcher.CloseAsync()
	if err != nil {
		return err
	}
	return p.batcher.WaitForClose(time.Until(tstop))
}

//------------------------------------------------------------------------------
