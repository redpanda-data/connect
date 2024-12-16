package spannercdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"golang.org/x/sync/errgroup"
)

// PartitionStorage is a storage interface for partition metadata.
type PartitionStorage interface {
	GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error)
	GetInterruptedPartitions(ctx context.Context) ([]*PartitionMetadata, error)
	InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error
	GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error)
	AddChildPartitions(ctx context.Context, parentPartition *PartitionMetadata, childPartitionsRecord *ChildPartitionsRecord) error
	UpdateToScheduled(ctx context.Context, partitions []*PartitionMetadata) error
	UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error
	UpdateToFinished(ctx context.Context, partition *PartitionMetadata) error
	UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error
}

// Subscriber subscribes change stream.
type Subscriber struct {
	spannerClient          *spanner.Client
	streamName             string
	startTimestamp         time.Time
	endTimestamp           time.Time
	heartbeatInterval      time.Duration
	spannerRequestPriority spannerpb.RequestOptions_Priority
	partitionStorage       PartitionStorage
	consumer               Consumer
	eg                     *errgroup.Group
	mu                     sync.Mutex
}

type config struct {
	startTimestamp         time.Time
	endTimestamp           time.Time
	heartbeatInterval      time.Duration
	spannerRequestPriority spannerpb.RequestOptions_Priority
}

// Option interface for subscriber.
type Option interface {
	Apply(*config)
}

type withStartTimestamp time.Time

func (o withStartTimestamp) Apply(c *config) {
	c.startTimestamp = time.Time(o)
}

// WithStartTimestamp set the start timestamp option for read change streams.
//
// The value must be within the retention period of the change stream and before the current time.
// Default value is current timestamp.
func WithStartTimestamp(startTimestamp time.Time) Option {
	return withStartTimestamp(startTimestamp)
}

type withEndTimestamp time.Time

func (o withEndTimestamp) Apply(c *config) {
	c.endTimestamp = time.Time(o)
}

// WithEndTimestamp set the end timestamp option for read change streams.
//
// The value must be within the retention period of the change stream and must be after the start timestamp.
// If not set, read latest changes until canceled.
func WithEndTimestamp(endTimestamp time.Time) Option {
	return withEndTimestamp(endTimestamp)
}

type withHeartbeatInterval time.Duration

func (o withHeartbeatInterval) Apply(c *config) {
	c.heartbeatInterval = time.Duration(o)
}

// WithHeartbeatInterval set the heartbeat interval for read change streams.
//
// Default value is 10 seconds.
func WithHeartbeatInterval(heartbeatInterval time.Duration) Option {
	return withHeartbeatInterval(heartbeatInterval)
}

type withSpannerRequestPriotiry spannerpb.RequestOptions_Priority

func (o withSpannerRequestPriotiry) Apply(c *config) {
	c.spannerRequestPriority = spannerpb.RequestOptions_Priority(o)
}

// WithSpannerRequestPriotiry set the request priority option for read change streams.
//
// Default value is unspecified, equivalent to high.
func WithSpannerRequestPriotiry(priority spannerpb.RequestOptions_Priority) Option {
	return withSpannerRequestPriotiry(priority)
}

var (
	defaultEndTimestamp      = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC) // Maximum value of Spanner TIMESTAMP type.
	defaultHeartbeatInterval = 10 * time.Second

	nowFunc = time.Now
)

// NewSubscriber creates a new subscriber of change streams.
func NewSubscriber(
	client *spanner.Client,
	streamName string,
	partitionStorage PartitionStorage,
	options ...Option,
) *Subscriber {
	c := &config{
		startTimestamp:    nowFunc(),
		endTimestamp:      defaultEndTimestamp,
		heartbeatInterval: defaultHeartbeatInterval,
	}
	for _, o := range options {
		o.Apply(c)
	}

	return &Subscriber{
		spannerClient:          client,
		streamName:             streamName,
		startTimestamp:         c.startTimestamp,
		endTimestamp:           c.endTimestamp,
		heartbeatInterval:      c.heartbeatInterval,
		spannerRequestPriority: c.spannerRequestPriority,
		partitionStorage:       partitionStorage,
	}
}

// Subscribe starts subscribing to the change stream.
func (s *Subscriber) Subscribe(ctx context.Context, consumer Consumer) error {
	eg, ctx := s.initErrGroup(ctx)
	s.consumer = consumer

	// Initialize root partition if this is the first run or if the previous run has already been completed.
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watermark partition on start subscribe: %w", err)
	}
	if minWatermarkPartition == nil {
		if err := s.partitionStorage.InitializeRootPartition(ctx, s.startTimestamp, s.endTimestamp, s.heartbeatInterval); err != nil {
			return fmt.Errorf("failed to initialize root partition: %w", err)
		}
	}

	interruptedPartitions, err := s.partitionStorage.GetInterruptedPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get interrupted partitions: %w", err)
	}
	for _, p := range interruptedPartitions {
		p := p
		s.eg.Go(func() error {
			return s.queryChangeStream(ctx, p)
		})
	}

	eg.Go(func() error {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.detectNewPartitions(ctx)
				switch err {
				case errDone:
					return nil
				case nil:
					// continue
				default:
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return eg.Wait()
}

// SubscribeFunc is an adapter to allow the use of ordinary functions as Consumer.
//
// function might be called from multiple goroutines and must be re-entrant safe.
func (s *Subscriber) SubscribeFunc(ctx context.Context, f ConsumerFunc) error {
	return s.Subscribe(ctx, f)
}

func (s *Subscriber) initErrGroup(ctx context.Context) (*errgroup.Group, context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.eg != nil {
		panic("Subscriber has already started subscribe.")
	}

	eg, ctx := errgroup.WithContext(ctx)
	s.eg = eg
	return eg, ctx
}

var errDone = errors.New("all partitions have been processed")

func (s *Subscriber) detectNewPartitions(ctx context.Context) error {
	minWatermarkPartition, err := s.partitionStorage.GetUnfinishedMinWatermarkPartition(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unfinished min watarmark partition: %w", err)
	}

	if minWatermarkPartition == nil {
		return errDone
	}

	// To make sure changes for a key is processed in timestamp order, wait until the records returned from all parents have been processed.
	partitions, err := s.partitionStorage.GetSchedulablePartitions(ctx, minWatermarkPartition.Watermark)
	if err != nil {
		return fmt.Errorf("failed to get schedulable partitions: %w", err)
	}
	if len(partitions) == 0 {
		return nil
	}

	if err := s.partitionStorage.UpdateToScheduled(ctx, partitions); err != nil {
		return fmt.Errorf("failed to update to scheduled: %w", err)
	}

	for _, p := range partitions {
		p := p
		s.eg.Go(func() error {
			return s.queryChangeStream(ctx, p)
		})
	}

	return nil
}

func (s *Subscriber) queryChangeStream(ctx context.Context, p *PartitionMetadata) error {
	if err := s.partitionStorage.UpdateToRunning(ctx, p); err != nil {
		return fmt.Errorf("failed to update to running: %w", err)
	}

	stmt := spanner.Statement{
		SQL: fmt.Sprintf("SELECT ChangeRecord FROM READ_%s (@startTimestamp, @endTimestamp, @partitionToken, @heartbeatMilliseconds)", s.streamName),
		Params: map[string]interface{}{
			"startTimestamp":        p.Watermark,
			"endTimestamp":          p.EndTimestamp,
			"partitionToken":        p.PartitionToken,
			"heartbeatMilliseconds": p.HeartbeatMillis,
		},
	}

	if p.IsRootPartition() {
		// Must be converted to NULL (root partition).
		stmt.Params["partitionToken"] = nil
	}

	iter := s.spannerClient.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.spannerRequestPriority})
	if err := iter.Do(func(r *spanner.Row) error {
		records := []*ChangeRecord{}
		if err := r.Columns(&records); err != nil {
			return err
		}
		if err := s.handle(ctx, p, records); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if err := s.partitionStorage.UpdateToFinished(ctx, p); err != nil {
		return fmt.Errorf("failed to update to finished: %w", err)
	}

	return nil
}

type watermarker struct {
	watermark time.Time
}

func (w *watermarker) set(t time.Time) {
	if t.After(w.watermark) {
		w.watermark = t
	}
}

func (w *watermarker) get() time.Time {
	return w.watermark
}

func (s *Subscriber) handle(ctx context.Context, p *PartitionMetadata, records []*ChangeRecord) error {
	var watermarker watermarker
	for _, cr := range records {
		for _, record := range cr.DataChangeRecords {
			if err := s.consumer.Consume(record.DecodeToNonSpannerType()); err != nil {
				return err
			}
			watermarker.set(record.CommitTimestamp)
		}
		for _, record := range cr.HeartbeatRecords {
			watermarker.set(record.Timestamp)
		}
		for _, record := range cr.ChildPartitionsRecords {
			if err := s.partitionStorage.AddChildPartitions(ctx, p, record); err != nil {
				return fmt.Errorf("failed to add child partitions: %w", err)
			}
			watermarker.set(record.StartTimestamp)
		}
	}

	if err := s.partitionStorage.UpdateWatermark(ctx, p, watermarker.get()); err != nil {
		return fmt.Errorf("failed to update watermark: %w", err)
	}

	return nil
}
