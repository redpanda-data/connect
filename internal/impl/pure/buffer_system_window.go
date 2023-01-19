package pure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func tumblingWindowBufferConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.53.0").
		Categories("Windowing").
		Summary("Chops a stream of messages into tumbling or sliding windows of fixed temporal size, following the system clock.").
		Description(`
A window is a grouping of messages that fit within a discrete measure of time following the system clock. Messages are allocated to a window either by the processing time (the time at which they're ingested) or by the event time, and this is controlled via the `+"[`timestamp_mapping` field](#timestamp_mapping)"+`.

In tumbling mode (default) the beginning of a window immediately follows the end of a prior window. When the buffer is initialized the first window to be created and populated is aligned against the zeroth minute of the zeroth hour of the day by default, and may therefore be open for a shorter period than the specified size.

A window is flushed only once the system clock surpasses its scheduled end. If an `+"[`allowed_lateness`](#allowed_lateness)"+` is specified then the window will not be flushed until the scheduled end plus that length of time.

When a message is added to a window it has a metadata field `+"`window_end_timestamp`"+` added to it containing the timestamp of the end of the window as an RFC3339 string.

## Sliding Windows

Sliding windows begin from an offset of the prior windows' beginning rather than its end, and therefore messages may belong to multiple windows. In order to produce sliding windows specify a `+"[`slide` duration](#slide)"+`.

## Back Pressure

If back pressure is applied to this buffer either due to output services being unavailable or resources being saturated, windows older than the current and last according to the system clock will be dropped in order to prevent unbounded resource usage. This means you should ensure that under the worst case scenario you have enough system memory to store two windows' worth of data at a given time (plus extra for redundancy and other services).

If messages could potentially arrive with event timestamps in the future (according to the system clock) then you should also factor in these extra messages in memory usage estimates.

## Delivery Guarantees

This buffer honours the transaction model within Benthos in order to ensure that messages are not acknowledged until they are either intentionally dropped or successfully delivered to outputs. However, since messages belonging to an expired window are intentionally dropped there are circumstances where not all messages entering the system will be delivered.

When this buffer is configured with a slide duration it is possible for messages to belong to multiple windows, and therefore be delivered multiple times. In this case the first time the message is delivered it will be acked (or nacked) and subsequent deliveries of the same message will be a "best attempt".

During graceful termination if the current window is partially populated with messages they will be nacked such that they are re-consumed the next time the service starts.
`).
		Field(service.NewBloblangField("timestamp_mapping").
			Description(`
A [Bloblang mapping](/docs/guides/bloblang/about) applied to each message during ingestion that provides the timestamp to use for allocating it a window. By default the function `+"`now()`"+` is used in order to generate a fresh timestamp at the time of ingestion (the processing time), whereas this mapping can instead extract a timestamp from the message itself (the event time).

The timestamp value assigned to `+"`root`"+` must either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in ISO 8601 format. If the mapping fails or provides an invalid result the message will be dropped (with logging to describe the problem).
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
			Description("An optional duration string to offset the beginning of each window by, otherwise they are aligned to the zeroth minute and zeroth hour on the UTC clock. The offset cannot be a larger or equal measure to the window size or the slide.").
			Default("").
			Example("-6h").Example("30m")).
		Field(service.NewStringField("allowed_lateness").
			Description("An optional duration string describing the length of time to wait after a window has ended before flushing it, allowing late arrivals to be included. Since this windowing buffer uses the system clock an allowed lateness can improve the matching of messages when using event time.").
			Default("").
			Example("10s").Example("1m")).
		Example("Counting Passengers at Traffic", `Given a stream of messages relating to cars passing through various traffic lights of the form:

`+"```json"+`
{
  "traffic_light": "cbf2eafc-806e-4067-9211-97be7e42cee3",
  "created_at": "2021-08-07T09:49:35Z",
  "registration_plate": "AB1C DEF",
  "passengers": 3
}
`+"```"+`

We can use a window buffer in order to create periodic messages summarising the traffic for a period of time of this form:

`+"```json"+`
{
  "traffic_light": "cbf2eafc-806e-4067-9211-97be7e42cee3",
  "created_at": "2021-08-07T10:00:00Z",
  "total_cars": 15,
  "passengers": 43
}
`+"```"+`

With the following config:`,
			`
buffer:
  system_window:
    timestamp_mapping: root = this.created_at
    size: 1h

pipeline:
  processors:
    # Group messages of the window into batches of common traffic light IDs
    - group_by_value:
        value: '${! json("traffic_light") }'

    # Reduce each batch to a single message by deleting indexes > 0, and
    # aggregate the car and passenger counts.
    - mapping: |
        root = if batch_index() == 0 {
          {
            "traffic_light": this.traffic_light,
            "created_at": meta("window_end_timestamp"),
            "total_cars": json("registration_plate").from_all().unique().length(),
            "passengers": json("passengers").from_all().sum(),
          }
        } else { deleted() }
`,
		)
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
			if slide > 0 && offset >= slide {
				return nil, fmt.Errorf("invalid offset '%v' must be lower than the slide '%v'", offset, slide)
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
	ts    time.Time
	m     *service.Message
	ackFn service.AckFunc
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

	closedTimerChan <-chan time.Time

	endOfInputChan      chan struct{}
	closeEndOfInputOnce sync.Once
}

func newSystemWindowBuffer(
	tsMapping *bloblang.Executor,
	clock utcNowProvider,
	size, slide, offset, allowedLateness time.Duration,
	logger *service.Logger,
) (*systemWindowBuffer, error) {
	w := &systemWindowBuffer{
		tsMapping:       tsMapping,
		clock:           clock,
		size:            size,
		slide:           slide,
		allowedLateness: allowedLateness,
		offset:          offset,
		logger:          logger,
		oldestTS:        clock(),
		endOfInputChan:  make(chan struct{}),
	}

	tmpTimerChan := make(chan time.Time)
	close(tmpTimerChan)
	w.closedTimerChan = tmpTimerChan
	return w, nil
}

func (w *systemWindowBuffer) nextSystemWindow() (prevStart, prevEnd, start, end time.Time) {
	now := w.clock()

	windowEpoch := w.size
	if w.slide > 0 {
		windowEpoch = w.slide
	}

	// The start is now, rounded by our window epoch to the UTC clock, and with
	// our offset (plus one to avoid overlapping with the previous window)
	// added.
	//
	// If the result is after now then we rounded upwards, so we roll it back by
	// the window epoch.
	if start = w.clock().Round(windowEpoch).Add(1 + w.offset); start.After(now) {
		start = start.Add(-windowEpoch)
	}

	// The result is the start of the newest active window. In the case of
	// sliding windows this is not the "next" window to be flushed, so we need
	// to roll back further.
	if w.slide > 0 {
		start = start.Add(w.slide - w.size)
	}

	// The end is our start plus the window size (minus the nanosecond added to
	// the start).
	end = start.Add(w.size - 1)

	// Calculate the previous window as well
	prevStart, prevEnd = start.Add(-windowEpoch), end.Add(-windowEpoch)
	return
}

func (w *systemWindowBuffer) getTimestamp(i int, batch service.MessageBatch) (ts time.Time, err error) {
	var tsValueMsg *service.Message
	if tsValueMsg, err = batch.BloblangQuery(i, w.tsMapping); err != nil {
		w.logger.Errorf("Timestamp mapping failed for message: %v", err)
		err = fmt.Errorf("timestamp mapping failed: %w", err)
		return
	}

	var tsValue any
	if tsValue, err = tsValueMsg.AsStructured(); err != nil {
		if tsBytes, _ := tsValueMsg.AsBytes(); len(tsBytes) > 0 {
			tsValue = string(tsBytes)
			err = nil
		}
	}
	if err != nil {
		w.logger.Errorf("Timestamp mapping failed for message: unable to parse result as structured value: %v", err)
		err = fmt.Errorf("unable to parse result of timestamp mapping as structured value: %w", err)
		return
	}

	if ts, err = query.IGetTimestamp(tsValue); err != nil {
		w.logger.Errorf("Timestamp mapping failed for message: %v", err)
		err = fmt.Errorf("unable to parse result of timestamp mapping as timestamp: %w", err)
	}
	return
}

func (w *systemWindowBuffer) WriteBatch(ctx context.Context, msgBatch service.MessageBatch, aFn service.AckFunc) error {
	w.pendingMut.Lock()
	defer w.pendingMut.Unlock()

	// If our output is blocked and therefore we haven't flushed more than the
	// last two windows we purge messages that wouldn't fit within them.
	prevStart, _, _, _ := w.nextSystemWindow()
	if w.latestFlushedWindowEnd.Before(prevStart) && w.oldestTS.Before(prevStart) {
		newOldestTS := w.clock()
		newPending := make([]*tsMessage, 0, len(w.pending))
		for _, pending := range w.pending {
			if pending.ts.Before(prevStart) {
				// Reject messages too old to fit into a window by acknowledging
				// them.
				_ = pending.ackFn(ctx, nil)
				continue
			}
			newPending = append(newPending, pending)
			if pending.ts.Before(newOldestTS) {
				newOldestTS = pending.ts
			}
		}
		w.pending = newPending
	}

	messageAdded := false
	aggregatedAck := batch.NewCombinedAcker(batch.AckFunc(aFn))

	// And now add new messages.
	for i, msg := range msgBatch {
		ts, err := w.getTimestamp(i, msgBatch)
		if err != nil {
			return err
		}

		// Don't add messages older than our current window start.
		if !ts.After(w.latestFlushedWindowEnd) { //nolint: gocritic
			continue
		}

		messageAdded = true
		w.pending = append(w.pending, &tsMessage{
			ts: ts, m: msg, ackFn: service.AckFunc(aggregatedAck.Derive()),
		})
		if ts.Before(w.oldestTS) {
			w.oldestTS = ts
		}
	}

	if !messageAdded {
		// If none of the messages have fit into a window we reject them by
		// acknowledging the batch.
		_ = aFn(ctx, nil)
	}
	return nil
}

func (w *systemWindowBuffer) flushWindow(ctx context.Context, start, end time.Time) (service.MessageBatch, service.AckFunc, error) {
	w.pendingMut.Lock()
	defer w.pendingMut.Unlock()

	// Calculate the next start and purge everything older as we flush.
	nextStart := start.Add(w.size)
	if w.slide > 0 {
		nextStart = start.Add(w.slide)
	}

	var flushBatch service.MessageBatch
	var flushAcks []service.AckFunc

	newPending := make([]*tsMessage, 0, len(w.pending))
	newOldest := w.clock()
	for _, pending := range w.pending {
		flush := !pending.ts.Before(start) && !pending.ts.After(end) //nolint: gocritic
		preserve := !pending.ts.Before(nextStart)                    //nolint: gocritic

		if flush {
			tmpMsg := pending.m.Copy()
			tmpMsg.MetaSet("window_end_timestamp", end.Format(time.RFC3339Nano))
			flushBatch = append(flushBatch, tmpMsg)
			flushAcks = append(flushAcks, pending.ackFn)
		}
		if preserve {
			if pending.ts.Before(newOldest) {
				newOldest = pending.ts
			}
			newPending = append(newPending, pending)
		}
		if !flush && !preserve {
			_ = pending.ackFn(ctx, nil)
		}
	}

	w.pending = newPending
	w.latestFlushedWindowEnd = end
	w.oldestTS = newOldest

	return flushBatch, func(ctx context.Context, err error) error {
		for _, aFn := range flushAcks {
			_ = aFn(ctx, err)
		}
		return nil
	}, nil
}

var errWindowClosed = errors.New("message rejected as window did not complete")

func (w *systemWindowBuffer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	prevStart, prevEnd, nextStart, nextEnd := w.nextSystemWindow()

	// We haven't been read since the previous window ended, so create that one
	// instead in an attempt to back fill.
	//
	// Note that we do not need to lock around latestFlushWindowEnd because it's
	// only written from the reader, and we only expect one active reader at a
	// given time. If this assumption changes we would need to lock around this
	// also.
	if w.latestFlushedWindowEnd.Before(prevStart) {
		if msgBatch, aFn, err := w.flushWindow(ctx, prevStart, prevEnd); len(msgBatch) > 0 || err != nil {
			return msgBatch, aFn, err
		}
	}

	for {
		nextEndChan := w.closedTimerChan
		if waitFor := nextEnd.Sub(w.clock()) + w.allowedLateness; waitFor > 0 {
			nextEndChan = time.After(waitFor)
		}

		select {
		case <-nextEndChan:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-w.endOfInputChan:
			// Nack all pending messages so that we re-consume them on the next
			// start up. TODO: Eventually allow users to customize this as they
			// may wish to flush partial windows instead.
			w.pendingMut.Lock()
			for _, pending := range w.pending {
				_ = pending.ackFn(ctx, errWindowClosed)
			}
			w.pending = nil
			w.pendingMut.Unlock()
			return nil, nil, service.ErrEndOfBuffer
		}
		if msgBatch, aFn, err := w.flushWindow(ctx, nextStart, nextEnd); len(msgBatch) > 0 || err != nil {
			return msgBatch, aFn, err
		}

		// Window did not contain any messages, so move onto next.
		_, _, nextStart, nextEnd = w.nextSystemWindow()
	}
}

func (w *systemWindowBuffer) EndOfInput() {
	w.closeEndOfInputOnce.Do(func() {
		close(w.endOfInputChan)
	})
}

func (w *systemWindowBuffer) Close(ctx context.Context) error {
	return nil
}
