// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// resources.go defines the salesforceProc salesforceProcessor struct and implements the resource dispatcher.
// The searchResource function routes incoming queries to the appropriate
// Salesforce resource handler (issues, projects, users, roles, etc.).

package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcegrpc"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

// QueryType selects which Salesforce API implementation to use.
type QueryType int

// QueryREST represents the REST query mode for Salesforce operations.
const (
	QueryREST QueryType = iota

	// QueryGraphQL represents the GraphQL query mode.
	QueryGraphQL
)

// FilterConfig represents optional user-provided filters.
type FilterConfig struct {
	Enabled bool
	Value   string
}

// Request contains everything needed to run a Salesforce query.
type Request struct {
	QueryType QueryType
	Filter    FilterConfig
}

// ProcessorState holds the full checkpoint state for the Salesforce processor,
// covering both the REST snapshot phase and the CDC/Pub/Sub streaming phase.
type ProcessorState struct {
	RestCursor       salesforcehttp.Cursor `json:"rest_cursor"`
	SnapshotComplete bool                  `json:"snapshot_complete"`
	CDCReplayID      []byte                `json:"cdc_replay_id,omitempty"`
	PubSubReplayID   []byte                `json:"pubsub_replay_id,omitempty"`
	PubSubTopic      string                `json:"pubsub_topic,omitempty"`
}

// Dispatch executes the correct Salesforce flow.
func (s *salesforceProcessor) Dispatch(
	ctx context.Context,
	req Request,
) (service.MessageBatch, error) {
	// Standalone Platform Events mode: pubsub_topic is set without cdc_enabled
	// and there's no REST filter — go straight to streaming.
	if s.pubsubTopic != "" && !s.cdcEnabled && !req.Filter.Enabled {
		state, err := s.loadState(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load checkpoint: %w", err)
		}
		return s.dispatchPubSub(ctx, state)
	}

	switch req.QueryType {

	case QueryREST:
		if !req.Filter.Enabled {
			return s.dispatchWithCheckpoint(ctx)
		}
		return s.client.RestQueryFiltered(ctx, req.Filter.Value)

	case QueryGraphQL:
		if !req.Filter.Enabled {
			return nil, errors.New("unhandled query type: QueryGraphQL - not filtered")
		}
		return s.client.GraphQLQueryFiltered(ctx, req.Filter.Value)

	default:
		return nil, fmt.Errorf("unhandled query type: %v", req.QueryType)
	}
}

// dispatchWithCheckpoint fetches the next batch using cursor-based checkpointing.
// When the REST snapshot completes and CDC is enabled, it transitions to CDC streaming.
func (s *salesforceProcessor) dispatchWithCheckpoint(ctx context.Context) (service.MessageBatch, error) {
	state, err := s.loadState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// If snapshot is complete and CDC is enabled, dispatch CDC events
	if state.SnapshotComplete && s.cdcEnabled {
		// If pubsub_topic is set, skip REST snapshot and go straight to streaming
		if s.pubsubTopic != "" {
			return s.dispatchPubSub(ctx, state)
		}
		return s.dispatchCDC(ctx, state)
	}

	// If snapshot is already complete but CDC is not enabled, reset for next scan
	if state.SnapshotComplete {
		state.SnapshotComplete = false
		state.RestCursor = salesforcehttp.Cursor{}
		if err := s.saveState(ctx, state); err != nil {
			s.log.Errorf("failed to reset state after completion: %v", err)
		}
		return nil, nil
	}

	batch, nextCursor, done, err := s.client.GetNextBatch(ctx, state.RestCursor)
	if err != nil {
		return nil, fmt.Errorf("GetNextBatch failed: %w", err)
	}

	if done {
		// All SObjects processed; mark snapshot as complete
		state.SnapshotComplete = true
		state.RestCursor = salesforcehttp.Cursor{}
		if err := s.saveState(ctx, state); err != nil {
			s.log.Errorf("failed to save snapshot completion: %v", err)
		}

		if s.cdcEnabled {
			s.log.Info("REST snapshot complete, transitioning to CDC streaming")
			return nil, nil
		}

		// CDC not enabled: reset for next full scan
		s.log.Info("All Salesforce records processed, waiting for next poll interval")
		state.SnapshotComplete = false
		if err := s.saveState(ctx, state); err != nil {
			s.log.Errorf("failed to reset checkpoint after completion: %v", err)
		}
		return nil, nil
	}

	// Persist checkpoint so we can resume from here on restart
	state.RestCursor = nextCursor
	if err := s.saveState(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to save checkpoint: %w", err)
	}

	return batch, nil
}

// dispatchCDC handles the CDC streaming phase after the REST snapshot is complete.
// The client handles reconnection internally; this method only needs to check
// for permanent stream failures.
func (s *salesforceProcessor) dispatchCDC(ctx context.Context, state ProcessorState) (service.MessageBatch, error) {
	// Lazy-init gRPC client if nil
	if s.grpcClient == nil {
		if err := s.initGRPCClient(ctx, state.CDCReplayID); err != nil {
			return nil, fmt.Errorf("failed to init CDC gRPC client: %w", err)
		}
	}

	// Check for permanent stream errors (the client handles transient reconnection internally)
	if err := s.grpcClient.StreamErr(); err != nil {
		grpcErr, isGRPC := status.FromError(err)

		// Handle stale replay_id (>72h)
		if isGRPC && grpcErr.Code() == codes.InvalidArgument {
			s.log.Warn("CDC replay_id appears stale, falling back to configured preset")
			_ = s.grpcClient.Close()
			s.grpcClient = nil
			state.CDCReplayID = nil
			if saveErr := s.saveState(ctx, state); saveErr != nil {
				s.log.Errorf("failed to clear stale replay_id: %v", saveErr)
			}
			return nil, nil
		}

		// Permanent failure — return the error
		return nil, fmt.Errorf("CDC stream permanent failure: %w", err)
	}

	events, latestReplayID, err := s.grpcClient.FetchBatch(int(s.cdcBatchSize))
	if err != nil {
		return nil, fmt.Errorf("CDC FetchBatch failed: %w", err)
	}

	if len(events) == 0 {
		return nil, nil
	}

	batch := s.eventsToMessageBatch(events)

	// Checkpoint the replay_id
	if len(latestReplayID) > 0 {
		state.CDCReplayID = latestReplayID
		if err := s.saveState(ctx, state); err != nil {
			return nil, fmt.Errorf("failed to checkpoint CDC replay_id: %w", err)
		}
	}

	return batch, nil
}

// dispatchPubSub handles standalone Pub/Sub streaming (Platform Events or arbitrary topics).
func (s *salesforceProcessor) dispatchPubSub(ctx context.Context, state ProcessorState) (service.MessageBatch, error) {
	// Lazy-init gRPC client if nil
	if s.grpcClient == nil {
		replayID := state.PubSubReplayID
		if err := s.initGRPCClient(ctx, replayID); err != nil {
			return nil, fmt.Errorf("failed to init Pub/Sub gRPC client: %w", err)
		}
	}

	// Check for permanent stream errors
	if err := s.grpcClient.StreamErr(); err != nil {
		grpcErr, isGRPC := status.FromError(err)

		if isGRPC && grpcErr.Code() == codes.InvalidArgument {
			s.log.Warn("Pub/Sub replay_id appears stale, falling back to configured preset")
			_ = s.grpcClient.Close()
			s.grpcClient = nil
			state.PubSubReplayID = nil
			if saveErr := s.saveState(ctx, state); saveErr != nil {
				s.log.Errorf("failed to clear stale replay_id: %v", saveErr)
			}
			return nil, nil
		}

		return nil, fmt.Errorf("Pub/Sub stream permanent failure: %w", err)
	}

	events, latestReplayID, err := s.grpcClient.FetchBatch(int(s.cdcBatchSize))
	if err != nil {
		return nil, fmt.Errorf("Pub/Sub FetchBatch failed: %w", err)
	}

	if len(events) == 0 {
		return nil, nil
	}

	batch := s.eventsToMessageBatch(events)

	// Checkpoint the replay_id
	if len(latestReplayID) > 0 {
		state.PubSubReplayID = latestReplayID
		state.PubSubTopic = s.pubsubTopic
		if err := s.saveState(ctx, state); err != nil {
			return nil, fmt.Errorf("failed to checkpoint Pub/Sub replay_id: %w", err)
		}
	}

	return batch, nil
}

// eventsToMessageBatch converts PubSubEvents to a Benthos MessageBatch,
// setting appropriate metadata based on event type.
func (s *salesforceProcessor) eventsToMessageBatch(events []*salesforcegrpc.PubSubEvent) service.MessageBatch {
	var batch service.MessageBatch
	for _, evt := range events {
		payload, marshalErr := json.Marshal(evt.RawPayload)
		if marshalErr != nil {
			s.log.Errorf("Failed to marshal event payload: %v", marshalErr)
			continue
		}

		msg := service.NewMessage(payload)

		switch evt.Type {
		case salesforcegrpc.EventTypeCDC:
			// Filter by configured CDC objects
			if !s.filterCDCEntity(evt.EntityName) {
				continue
			}
			msg.MetaSet("cdc_event", "true")
			msg.MetaSet("change_type", evt.ChangeType)
			msg.MetaSet("entity_name", evt.EntityName)
			if len(evt.RecordIDs) > 0 {
				msg.MetaSet("record_ids", strings.Join(evt.RecordIDs, ","))
			}
		case salesforcegrpc.EventTypePlatform:
			msg.MetaSet("platform_event", "true")
			msg.MetaSet("topic_name", evt.TopicName)
		}

		batch = append(batch, msg)
	}
	return batch
}

// initGRPCClient creates and connects the gRPC Pub/Sub client using
// SubscriptionConfig and functional options.
func (s *salesforceProcessor) initGRPCClient(ctx context.Context, replayID []byte) error {
	// Ensure we have fresh auth
	if s.client.BearerToken() == "" {
		if err := s.client.RefreshToken(ctx); err != nil {
			return fmt.Errorf("failed to obtain auth for gRPC: %w", err)
		}
	}

	cfg := salesforcegrpc.SubscriptionConfig{
		TopicName:  s.cdcTopicName,
		BatchSize:  s.cdcBatchSize,
		BufferSize: int(s.cdcBufferSize),
	}

	grpcClient, err := salesforcegrpc.NewClient(
		s.log,
		s.client.InstanceURL(),
		s.client.TenantID(),
		s.client.BearerToken(),
		cfg,
		salesforcegrpc.WithBackoff(s.grpcReconnectBaseDelay, s.grpcReconnectMaxDelay, s.grpcReconnectMaxAttempts),
		salesforcegrpc.WithMetrics(s.res.Metrics()),
		salesforcegrpc.WithAuthRefresh(func() (string, string, string, error) {
			if err := s.client.RefreshToken(ctx); err != nil {
				return "", "", "", err
			}
			return s.client.BearerToken(), s.client.InstanceURL(), s.client.TenantID(), nil
		}),
	)
	if err != nil {
		return err
	}

	// Determine replay preset
	var replayPreset salesforcegrpc.ReplayPreset
	switch s.cdcReplayPreset {
	case "earliest":
		replayPreset = salesforcegrpc.ReplayPreset_EARLIEST
	default:
		replayPreset = salesforcegrpc.ReplayPreset_LATEST
	}

	if err := grpcClient.Connect(ctx, replayPreset, replayID); err != nil {
		_ = grpcClient.Close()
		return err
	}

	s.grpcClient = grpcClient
	return nil
}

func (s *salesforceProcessor) accessCache(
	ctx context.Context,
	key string,
) (string, error) {
	var lastOffset string
	var cacheErr error

	if err := s.res.AccessCache(ctx, s.binLogCache, func(cache service.Cache) {
		val, err := cache.Get(ctx, key)
		if err == nil {
			lastOffset = string(val)
		}
		cacheErr = err
	}); err != nil {
		return "", fmt.Errorf("unable to access cache: %w", err)
	}

	// cacheErr is non-nil only if Get failed with something other than "not found"
	if cacheErr != nil && !errors.Is(cacheErr, service.ErrKeyNotFound) {
		return "", cacheErr
	}

	return lastOffset, nil
}

func (s *salesforceProcessor) writeCache(
	ctx context.Context,
	key string,
	value string,
) error {
	var cacheErr error

	if err := s.res.AccessCache(ctx, s.binLogCache, func(cache service.Cache) {
		cacheErr = cache.Set(ctx, key, []byte(value), nil)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}

	if cacheErr != nil {
		return fmt.Errorf("unable to write cache key %q: %w", key, cacheErr)
	}

	return nil
}

// loadState loads the processor state from cache, with backward compatibility
// for the old "sf_cursor" key format.
func (s *salesforceProcessor) loadState(ctx context.Context) (ProcessorState, error) {
	// Try loading new format first
	raw, err := s.accessCache(ctx, "sf_state")
	if err != nil {
		return ProcessorState{}, nil
	}
	if raw != "" {
		var state ProcessorState
		if err := json.Unmarshal([]byte(raw), &state); err != nil {
			return ProcessorState{}, fmt.Errorf("failed to unmarshal state from cache: %w", err)
		}
		return state, nil
	}

	// Backward compatibility: try old "sf_cursor" key
	oldRaw, err := s.accessCache(ctx, "sf_cursor")
	if err != nil {
		return ProcessorState{}, nil
	}
	if oldRaw != "" {
		var cursor salesforcehttp.Cursor
		if err := json.Unmarshal([]byte(oldRaw), &cursor); err != nil {
			return ProcessorState{}, fmt.Errorf("failed to unmarshal old cursor from cache: %w", err)
		}
		s.log.Info("Migrating from old sf_cursor format to sf_state")
		state := ProcessorState{RestCursor: cursor}
		// Save in new format and clean up old key
		if saveErr := s.saveState(ctx, state); saveErr != nil {
			s.log.Errorf("failed to save migrated state: %v", saveErr)
		}
		return state, nil
	}

	return ProcessorState{}, nil
}

func (s *salesforceProcessor) saveState(ctx context.Context, state ProcessorState) error {
	b, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := s.writeCache(ctx, "sf_state", string(b)); err != nil {
		return fmt.Errorf("failed to write state to cache: %w", err)
	}
	return nil
}
