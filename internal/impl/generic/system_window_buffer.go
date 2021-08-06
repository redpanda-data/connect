package generic

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/service"
)

func tumblingWindowBufferConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Windowing").
		Summary("Chops a stream of messages into tumbling or sliding windows of fixed temporal size, following the system clock.").
		Description(`
A window is a grouping of messages that fit within a discrete measure of time. Messages are allocated to a window either by the processing time (the time at which they're ingested) or by the event time, and this is controlled via the ` + "[`timestamp_mapping` field](#timestamp_mapping)`" + `.

In tumbling mode (default) the beginning of a window immediately follows the end of a prior window. When the buffer is initialized the first window to be created and populated is aligned against the zeroth minute of the zeroth hour of the day by default, and may therefore be open for a shorter period than the specified size.

A window is flushed only once the system clock surpasses its scheduled end. If an ` + "[`allowed_lateness`](#allowed_lateness)" + ` is specified then the window will not be flushed until the scheduled end plus that length of time. When the service is shut down any partial windows will be dropped.

## Sliding Windows

Sliding windows begin from an offset of the prior windows' beginning rather than its end, and therefore messages may belong to multiple windows. In order to produce sliding windows specify a ` + "[`slide` duration](#slide)" + `.

## Back Pressure

If back pressure is applied to this buffer either due to output services being unavailable or resources being saturated, windows older than the current and last according to the system clock will be dropped in order to prevent unbounded resource usage. This means you should ensure that under the worst case scenario you have enough system memory to store two windows' worth of data at a given time (plus extra for redundancy and other services).

If messages could potentially arrive with event timestamps in the future (according to the system clock) then you should also factor in these extra messages in memory usage estimates.

## Delivery Guarantees

Using a buffer weakens the delivery guarantees of the pipeline by decoupling the acknowledgement of inputs from components downstream of the buffer. Therefore, in the event of server crashes or other rare faults there is no guarantee that messages are not lost when using this buffer. If you instead require strict delivery guarantees for your data consider instead using an [input broker with a batching policy](/docs/components/inputs/broker), with the ` + "`batching.period`" + ` field set to the window period.
`).
		Field(service.NewBloblangField("timestamp_mapping").
			Description(`
A [Bloblang mapping](/docs/guides/bloblang/about) applied to each message during ingestion that provides the timestamp to use for allocating it a window. By default the function ` + "`now()`" + ` is used in order to generate a fresh timestamp at the time of ingestion (the processing time), whereas this mapping can instead extract a timestamp from the message itself (the event time).

The timestamp value assigned to ` + "`root`" + ` must either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format. If the mapping fails or provides an invalid result the message will be dropped (with logging to describe the problem).
`).
			Default("root = now()").
			Example("root = this.created_at").Example(`root = meta("kafka_timestamp_unix").number()`)).
		Field(service.NewStringField("size").
			Description("A duration string describing the size of each window. By default windows are aligned to the zeroth minute and zeroth hour on the UTC clock, meaning windows of 1 hour duration will match the turn of each hour in the day, this can be adjusted with the `offset` field.").
			Example("30s").Example("10m")).
		Field(service.NewStringField("slide").
			Description("An optional duration string describing by how much time the beginning of each window should be offset from the beginning of the previous, and therefore creates sliding windows instead of tumbling. When specified this duration must be smaller than the `size` of the window.").
			Default("").
			Example("30s").Example("10m")).
		Field(service.NewStringField("offset").
			Description("An optional duration string to offset the beginning of each window by, otherwise they are aligned to the zeroth minute and zeroth hour on the UTC clock.").
			Default("").
			Example("-6h").Example("30m")).
		Field(service.NewStringField("allowed_lateness").
			Description("An optional duration string describing the length of time to wait after a window has ended before flushing it, allowing late arrivals to be included. Since this windowing buffer uses the system clock an allowed lateness can improve the matching of messages when using event time.").
			Default("").
			Example("10s").Example("1m"))
}

func getDuration(conf *service.ParsedConfig, required bool, name string) (time.Duration, error) {
	periodStr, err := conf.FieldString(name)
	if err != nil {
		return 0, err
	}
	if !required && periodStr == "" {
		return 0, nil
	}
	period, err := time.ParseDuration(periodStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse field '%v' as duration: %w", name, err)
	}
	return period, nil
}

func init() {
	err := service.RegisterBatchBuffer(
		"system_window", tumblingWindowBufferConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			size, err := getDuration(conf, true, "size")
			if err != nil {
				return nil, err
			}
			slide, err := getDuration(conf, false, "slide")
			if err != nil {
				return nil, err
			}
			if slide >= size {
				return nil, fmt.Errorf("invalid window slide '%v' must be lower than the size '%v'", slide, size)
			}
			offset, err := getDuration(conf, false, "offset")
			if err != nil {
				return nil, err
			}
			if offset >= size {
				return nil, fmt.Errorf("invalid offset '%v' must be lower than the size '%v'", offset, size)
			}
			allowedLateness, err := getDuration(conf, false, "allowed_lateness")
			if err != nil {
				return nil, err
			}
			if allowedLateness >= size {
				return nil, fmt.Errorf("invalid allowed_lateness '%v' must be lower than the size '%v'", allowedLateness, size)
			}
			tsMapping, err := conf.FieldBloblang("timestamp_mapping")
			if err != nil {
				return nil, err
			}
			return newSystemWindowBuffer(tsMapping, func() time.Time {
				return time.Now().UTC()
			}, size, slide, offset, allowedLateness, mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type tsMessage struct {
	ts time.Time
	m  *service.Message
}

type utcNowProvider func() time.Time

type systemWindowBuffer struct {
	logger *service.Logger

	tsMapping                            *bloblang.Executor
	clock                                utcNowProvider
	size, slide, offset, allowedLateness time.Duration

	latestFlushedWindowEnd time.Time
	oldestTS               time.Time
	pending                []*tsMessage
	pendingMut             sync.Mutex

	endOfInputChan      chan struct{}
	closeEndOfInputOnce sync.Once
}

func newSystemWindowBuffer(
	tsMapping *bloblang.Executor,
	clock utcNowProvider,
	size, slide, offset, allowedLateness time.Duration,
	logger *service.Logger,
) (*systemWindowBuffer, error) {
	return &systemWindowBuffer{
		tsMapping:       tsMapping,
		clock:           clock,
		size:            size,
		slide:           slide,
		offset:          offset,
		allowedLateness: allowedLateness,
		logger:          logger,
		oldestTS:        clock(),
		endOfInputChan:  make(chan struct{}),
	}, nil
}

func (w *systemWindowBuffer) currentSystemWindow() (prevStart, prevEnd, start, end time.Time) {
	now := w.clock()

	startEpoch := w.size
	if w.slide > 0 {
		startEpoch = w.slide
	}

	start = w.clock().Round(startEpoch)
	if start.After(now) {
		// We were rounded up, so set us back
		start = start.Add(-startEpoch)
	}

	// Add offset and roll back if we're beyond now
	if start = start.Add(w.offset); start.After(now) {
		start = start.Add(-w.size)
	}

	// Window end is minus one nanosecond so that our windows do not overlap
	end = start.Add(w.size - 1)

	// Calculate the previous window as well
	prevOffset := w.size
	if w.slide > 0 {
		prevOffset = w.slide
	}

	prevStart, prevEnd = start.Add(-prevOffset), end.Add(-prevOffset)
	return
}

func (w *systemWindowBuffer) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	w.pendingMut.Lock()
	defer w.pendingMut.Unlock()

	// If our output is blocked and therefore we haven't flushed more than the
	// last two windows we purge messages that wouldn't fit within them.
	prevStart, _, _, _ := w.currentSystemWindow()
	if w.latestFlushedWindowEnd.Before(prevStart) && w.oldestTS.Before(prevStart) {
		newOldestTS := w.clock()
		newPending := make([]*tsMessage, 0, len(w.pending))
		for _, pending := range w.pending {
			if pending.ts.Before(prevStart) {
				continue
			}
			newPending = append(newPending, pending)
			if pending.ts.Before(newOldestTS) {
				newOldestTS = pending.ts
			}
		}
		w.pending = newPending
	}

	// And now add new messages.
	for _, msg := range batch {
		tsValueMsg, err := msg.BloblangQuery(w.tsMapping)
		if err != nil {
			w.logger.Errorf("Timestamp mapping failed for message: %w", err)
			continue
		}
		tsValue, err := tsValueMsg.AsStructured()
		if err != nil {
			w.logger.Errorf("Timestamp mapping failed for message: unable to parse result as structured value: %w", err)
			continue
		}
		ts, err := query.IGetTimestamp(tsValue)
		if err != nil {
			w.logger.Errorf("Timestamp mapping failed for message: %w", err)
			continue
		}
		// Don't add messages older than our current window start.
		if !ts.After(w.latestFlushedWindowEnd) {
			continue
		}
		w.pending = append(w.pending, &tsMessage{
			ts: ts, m: msg,
		})
		if ts.Before(w.oldestTS) {
			w.oldestTS = ts
		}
	}

	return nil
}

func (w *systemWindowBuffer) flushWindow(start, end time.Time) (service.MessageBatch, error) {
	w.pendingMut.Lock()
	defer w.pendingMut.Unlock()

	// Calculate the next start and purge everything older as we flush.
	nextStart := start.Add(w.size)
	if w.slide > 0 {
		nextStart = start.Add(w.slide)
	}

	var flushBatch service.MessageBatch
	newPending := make([]*tsMessage, 0, len(w.pending))
	newOldest := w.clock()
	for _, pending := range w.pending {
		if !pending.ts.Before(start) && !pending.ts.After(end) {
			flushBatch = append(flushBatch, pending.m) // TODO: Need copy here?
		}
		if !pending.ts.Before(nextStart) {
			if pending.ts.Before(newOldest) {
				newOldest = pending.ts
			}
			newPending = append(newPending, pending)
		}
	}

	w.pending = newPending
	w.latestFlushedWindowEnd = end
	w.oldestTS = newOldest

	return flushBatch, nil
}

func noopAck(context.Context, error) error {
	return nil
}

func (w *systemWindowBuffer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	prevStart, prevEnd, nextStart, nextEnd := w.currentSystemWindow()

	// We haven't been read since the previous window ended, so create that one
	// instead in an attempt to back fill.
	//
	// Note that we do not need to lock around latestFlushWindowEnd because it's
	// only written from the reader, and we only expect one active reader at a
	// given time. If this assumption changes we would need to lock around this
	// also.
	if w.latestFlushedWindowEnd.Before(prevStart) {
		if batch, err := w.flushWindow(prevStart, prevEnd); len(batch) > 0 || err != nil {
			return batch, noopAck, err
		}
	}

	if waitFor := time.Until(nextEnd) + w.allowedLateness; waitFor > 0 {
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-w.endOfInputChan:
			return nil, nil, service.ErrEndOfBuffer
		}
	}

	batch, err := w.flushWindow(nextStart, nextEnd)
	return batch, noopAck, err
}

func (w *systemWindowBuffer) EndOfInput() {
	w.closeEndOfInputOnce.Do(func() {
		close(w.endOfInputChan)
	})
}

func (w *systemWindowBuffer) Close(ctx context.Context) error {
	return nil
}
