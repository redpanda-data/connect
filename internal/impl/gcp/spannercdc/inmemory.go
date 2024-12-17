package spannercdc

import (
	"context"
	"sort"
	"sync"
	"time"
)

// InmemoryPartitionStorage implements PartitionStorage that stores PartitionMetadata in memory.
type InmemoryPartitionStorage struct {
	mu sync.Mutex
	m  map[string]*PartitionMetadata
}

// NewInmemory creates new instance of InmemoryPartitionStorage
func NewInmemory() *InmemoryPartitionStorage {
	return &InmemoryPartitionStorage{
		m: make(map[string]*PartitionMetadata),
	}
}

// GetUnfinishedMinWatermarkPartition returns the partition with the smallest watermark that is not finished.
func (s *InmemoryPartitionStorage) GetUnfinishedMinWatermarkPartition(ctx context.Context) (*PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*PartitionMetadata{}
	for _, p := range s.m {
		if p.State != StateFinished {
			partitions = append(partitions, p)
		}
	}

	if len(partitions) == 0 {
		return nil, nil
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Watermark.Before(partitions[j].Watermark) })
	return partitions[0], nil
}

// GetInterruptedPartitions unimplemented for memory store.
func (s *InmemoryPartitionStorage) GetInterruptedPartitions(ctx context.Context) ([]*PartitionMetadata, error) {
	// InmemoryPartitionStorage can't return any partitions
	return nil, nil
}

// InitializeRootPartition initializes the root partition.
func (s *InmemoryPartitionStorage) InitializeRootPartition(ctx context.Context, startTimestamp time.Time, endTimestamp time.Time, heartbeatInterval time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := &PartitionMetadata{
		PartitionToken:  RootPartitionToken,
		ParentTokens:    []string{},
		StartTimestamp:  startTimestamp,
		EndTimestamp:    endTimestamp,
		HeartbeatMillis: heartbeatInterval.Milliseconds(),
		State:           StateCreated,
		Watermark:       startTimestamp,
		CreatedAt:       time.Now(),
	}
	s.m[p.PartitionToken] = p

	return nil
}

// GetSchedulablePartitions returns all partitions that are created and have a start timestamp before minWatermark.
func (s *InmemoryPartitionStorage) GetSchedulablePartitions(ctx context.Context, minWatermark time.Time) ([]*PartitionMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := []*PartitionMetadata{}
	for _, p := range s.m {
		if p.State == StateCreated && !minWatermark.After(p.StartTimestamp) {
			partitions = append(partitions, p)
		}
	}

	return partitions, nil
}

// AddChildPartitions adds child partitions to the parent partition.
func (s *InmemoryPartitionStorage) AddChildPartitions(ctx context.Context, parent *PartitionMetadata, r *ChildPartitionsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range r.ChildPartitions {
		p := &PartitionMetadata{
			PartitionToken:  v.Token,
			ParentTokens:    v.ParentPartitionTokens,
			StartTimestamp:  r.StartTimestamp,
			EndTimestamp:    parent.EndTimestamp,
			HeartbeatMillis: parent.HeartbeatMillis,
			State:           StateCreated,
			Watermark:       r.StartTimestamp,
		}
		s.m[p.PartitionToken] = p
	}

	return nil
}

// UpdateToScheduled updates the partitions to scheduled state.
func (s *InmemoryPartitionStorage) UpdateToScheduled(ctx context.Context, partitions []*PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, p := range partitions {
		p = s.m[p.PartitionToken]
		p.ScheduledAt = &now
		p.State = StateScheduled
	}

	return nil
}

// UpdateToRunning updates the partition to a running state.
func (s *InmemoryPartitionStorage) UpdateToRunning(ctx context.Context, partition *PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partition.PartitionToken]
	p.RunningAt = &now
	p.State = StateRunning

	return nil
}

// UpdateToFinished updates the partition with a finished timestamp.
func (s *InmemoryPartitionStorage) UpdateToFinished(ctx context.Context, partition *PartitionMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	p := s.m[partition.PartitionToken]
	p.FinishedAt = &now
	p.State = StateFinished

	return nil
}

// UpdateWatermark updates the watermark of the partition.
func (s *InmemoryPartitionStorage) UpdateWatermark(ctx context.Context, partition *PartitionMetadata, watermark time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[partition.PartitionToken].Watermark = watermark

	return nil
}

// Assert that InmemoryPartitionStorage implements PartitionStorage.
var _ PartitionStorage = (*InmemoryPartitionStorage)(nil)
