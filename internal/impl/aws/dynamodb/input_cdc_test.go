// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestConvertTableRecordsToBatch_FailoverSkipsOldRecords(t *testing.T) {
	cutoff := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	older := cutoff.Add(-time.Minute)
	newer := cutoff.Add(time.Minute)
	records := []streamstypes.Record{
		{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: &older, SequenceNumber: aws.String("1")}},
		{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: &newer, SequenceNumber: aws.String("2")}},
	}
	skipped := service.MockResources().Metrics().NewCounter("test_skipped")
	batch := convertTableRecordsToBatch(records, "t", "shard-1", nil, cutoff, skipped)
	require.Len(t, batch, 1)
	seq, _ := batch[0].MetaGet("dynamodb_sequence_number")
	require.Equal(t, "2", seq)
}

// ApproximateCreationDateTime is second-granular, so records can share the
// cutoff second. The cutoff is the minimum foreign checkpoint time, so records
// in that exact second may not have been processed in the prior region; they
// must be replayed (at-least-once) rather than dropped. Only strictly-older
// records are skipped.
func TestConvertTableRecordsToBatch_FailoverKeepsBoundarySecond(t *testing.T) {
	cutoff := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	before := cutoff.Add(-time.Second)
	atCutoff := cutoff // same second as the cutoff -> must be kept
	after := cutoff.Add(time.Second)
	records := []streamstypes.Record{
		{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: &before, SequenceNumber: aws.String("1")}},
		{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: &atCutoff, SequenceNumber: aws.String("2")}},
		{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: &after, SequenceNumber: aws.String("3")}},
	}
	skipped := service.MockResources().Metrics().NewCounter("test_skipped_boundary")
	batch := convertTableRecordsToBatch(records, "t", "shard-1", nil, cutoff, skipped)
	require.Len(t, batch, 2)
	seq0, _ := batch[0].MetaGet("dynamodb_sequence_number")
	seq1, _ := batch[1].MetaGet("dynamodb_sequence_number")
	require.Equal(t, "2", seq0)
	require.Equal(t, "3", seq1)
}

func TestShouldSkipFailoverRecord(t *testing.T) {
	cutoff := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	rec := func(ts *time.Time) streamstypes.Record {
		return streamstypes.Record{Dynamodb: &streamstypes.StreamRecord{ApproximateCreationDateTime: ts}}
	}
	before := cutoff.Add(-time.Second)
	after := cutoff.Add(time.Second)

	require.True(t, shouldSkipFailoverRecord(rec(&before), cutoff), "strictly older record is skipped")
	require.False(t, shouldSkipFailoverRecord(rec(&cutoff), cutoff), "boundary-second record is kept (at-least-once)")
	require.False(t, shouldSkipFailoverRecord(rec(&after), cutoff), "newer record is kept")
	require.False(t, shouldSkipFailoverRecord(rec(&before), time.Time{}), "no cutoff -> never skip")
	require.False(t, shouldSkipFailoverRecord(rec(nil), cutoff), "missing timestamp -> never skip")
}

func TestGlobalTableConfigParsing(t *testing.T) {
	spec := dynamoDBCDCInputConfig()
	env := service.NewEnvironment()

	conf := `
tables: [mytable]
checkpoint_table: cps
global_table: true
global_table_replicas: [us-west-2, us-east-1]
region: us-east-1
`
	parsed, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	cfg, err := dynamoCDCInputConfigFromParsed(parsed)
	require.NoError(t, err)
	require.True(t, cfg.globalTable)
	require.Equal(t, []string{"us-west-2", "us-east-1"}, cfg.globalTableReplicas)
}

func TestGlobalTableValidation_EmptyReplicas(t *testing.T) {
	conf := dynamoDBCDCConfig{
		tables:              []string{"t"},
		checkpointTable:     "cps",
		globalTable:         true,
		globalTableReplicas: nil,
		startFrom:           "trim_horizon",
		batchSize:           100,
		snapshot:            snapshotConfig{mode: snapshotModeNone, segments: 1, batchSize: 100},
	}
	err := validateDynamoDBCDCConfig(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "global_table requires at least one replica region")
}

func TestConvertAttributeValue(t *testing.T) {
	tests := []struct {
		name     string
		input    streamstypes.AttributeValue
		expected any
	}{
		{
			name:     "string value",
			input:    &streamstypes.AttributeValueMemberS{Value: "test"},
			expected: "test",
		},
		{
			name:     "number value",
			input:    &streamstypes.AttributeValueMemberN{Value: "123"},
			expected: "123",
		},
		{
			name:     "boolean true",
			input:    &streamstypes.AttributeValueMemberBOOL{Value: true},
			expected: true,
		},
		{
			name:     "boolean false",
			input:    &streamstypes.AttributeValueMemberBOOL{Value: false},
			expected: false,
		},
		{
			name:     "null value",
			input:    &streamstypes.AttributeValueMemberNULL{Value: true},
			expected: nil,
		},
		{
			name:     "string set",
			input:    &streamstypes.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "number set",
			input:    &streamstypes.AttributeValueMemberNS{Value: []string{"1", "2", "3"}},
			expected: []string{"1", "2", "3"},
		},
		{
			name: "map value",
			input: &streamstypes.AttributeValueMemberM{Value: map[string]streamstypes.AttributeValue{
				"key1": &streamstypes.AttributeValueMemberS{Value: "value1"},
				"key2": &streamstypes.AttributeValueMemberN{Value: "42"},
			}},
			expected: map[string]any{
				"key1": "value1",
				"key2": "42",
			},
		},
		{
			name: "list value",
			input: &streamstypes.AttributeValueMemberL{Value: []streamstypes.AttributeValue{
				&streamstypes.AttributeValueMemberS{Value: "item1"},
				&streamstypes.AttributeValueMemberN{Value: "100"},
			}},
			expected: []any{"item1", "100"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertAttributeValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertAttributeMap(t *testing.T) {
	input := map[string]streamstypes.AttributeValue{
		"id":     &streamstypes.AttributeValueMemberS{Value: "123"},
		"count":  &streamstypes.AttributeValueMemberN{Value: "42"},
		"active": &streamstypes.AttributeValueMemberBOOL{Value: true},
		"metadata": &streamstypes.AttributeValueMemberM{Value: map[string]streamstypes.AttributeValue{
			"created": &streamstypes.AttributeValueMemberS{Value: "2024-01-01"},
		}},
	}

	result := convertAttributeMap(input)

	assert.Equal(t, "123", result["id"])
	assert.Equal(t, "42", result["count"])
	assert.Equal(t, true, result["active"])
	assert.IsType(t, map[string]any{}, result["metadata"])
	metadata := result["metadata"].(map[string]any)
	assert.Equal(t, "2024-01-01", metadata["created"])
}

// Regression test: Verify RWMutex allows concurrent reads.
func TestConcurrentShardReaderAccess(t *testing.T) {
	logger := service.MockResources().Logger()

	input := &dynamoDBCDCInput{
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {shardID: "shard-001", iterator: aws.String("iter-001"), exhausted: false},
			"shard-002": {shardID: "shard-002", iterator: aws.String("iter-002"), exhausted: false},
		},
		log: logger,
	}

	// Multiple goroutines should be able to read concurrently
	done := make(chan bool, 3)

	for range 3 {
		go func() {
			input.mu.RLock()
			count := len(input.shardReaders)
			input.mu.RUnlock()
			assert.Equal(t, 2, count)
			done <- true
		}()
	}

	for range 3 {
		<-done
	}
}

// Test that exhausted shards are properly handled.
func TestExhaustedShardHandling(t *testing.T) {
	input := &dynamoDBCDCInput{
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {
				shardID:   "shard-001",
				iterator:  nil, // Exhausted - no iterator
				exhausted: true,
			},
			"shard-002": {
				shardID:   "shard-002",
				iterator:  aws.String("iter-002"),
				exhausted: false,
			},
		},
	}

	// Count active readers
	input.mu.RLock()
	activeCount := 0
	for _, reader := range input.shardReaders {
		if !reader.exhausted && reader.iterator != nil {
			activeCount++
		}
	}
	input.mu.RUnlock()

	assert.Equal(t, 1, activeCount, "Only one shard should be active")
}

// Test cleanupExhaustedShards removes exhausted shards correctly.
func TestCleanupExhaustedShards(t *testing.T) {
	logger := service.MockResources().Logger()

	t.Run("removes only exhausted shards", func(t *testing.T) {
		input := &dynamoDBCDCInput{
			shardReaders: map[string]*dynamoDBShardReader{
				"shard-001": {shardID: "shard-001", exhausted: true},
				"shard-002": {shardID: "shard-002", exhausted: false},
				"shard-003": {shardID: "shard-003", exhausted: true},
				"shard-004": {shardID: "shard-004", exhausted: false},
			},
			log: logger,
			metrics: dynamoDBCDCMetrics{
				shardsTracked: service.MockResources().Metrics().NewGauge("test_shards"),
			},
		}

		activeShards := map[string]context.CancelFunc{
			"shard-001": func() {},
			"shard-003": func() {},
		}

		input.cleanupExhaustedShards(activeShards)

		// Should only have non-exhausted shards left
		assert.Len(t, input.shardReaders, 2)
		assert.Contains(t, input.shardReaders, "shard-002")
		assert.Contains(t, input.shardReaders, "shard-004")
		assert.NotContains(t, input.shardReaders, "shard-001")
		assert.NotContains(t, input.shardReaders, "shard-003")

		// Active shards should have been removed
		assert.Empty(t, activeShards)
	})

	t.Run("handles empty shard map", func(t *testing.T) {
		input := &dynamoDBCDCInput{
			shardReaders: map[string]*dynamoDBShardReader{},
			log:          logger,
			metrics: dynamoDBCDCMetrics{
				shardsTracked: service.MockResources().Metrics().NewGauge("test_shards"),
			},
		}

		activeShards := map[string]context.CancelFunc{}
		input.cleanupExhaustedShards(activeShards)

		assert.Empty(t, input.shardReaders)
	})

	t.Run("handles all exhausted shards", func(t *testing.T) {
		input := &dynamoDBCDCInput{
			shardReaders: map[string]*dynamoDBShardReader{
				"shard-001": {shardID: "shard-001", exhausted: true},
				"shard-002": {shardID: "shard-002", exhausted: true},
			},
			log: logger,
			metrics: dynamoDBCDCMetrics{
				shardsTracked: service.MockResources().Metrics().NewGauge("test_shards"),
			},
		}

		activeShards := map[string]context.CancelFunc{}
		input.cleanupExhaustedShards(activeShards)

		assert.Empty(t, input.shardReaders)
	})

	t.Run("handles no exhausted shards", func(t *testing.T) {
		input := &dynamoDBCDCInput{
			shardReaders: map[string]*dynamoDBShardReader{
				"shard-001": {shardID: "shard-001", exhausted: false},
				"shard-002": {shardID: "shard-002", exhausted: false},
			},
			log: logger,
			metrics: dynamoDBCDCMetrics{
				shardsTracked: service.MockResources().Metrics().NewGauge("test_shards"),
			},
		}

		activeShards := map[string]context.CancelFunc{}
		input.cleanupExhaustedShards(activeShards)

		assert.Len(t, input.shardReaders, 2)
	})
}

func TestParseTableTagFilter(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    map[string][]string
		expectError bool
	}{
		{
			name:  "single key single value",
			input: "env:prod",
			expected: map[string][]string{
				"env": {"prod"},
			},
		},
		{
			name:  "single key multiple values",
			input: "env:prod,staging,dev",
			expected: map[string][]string{
				"env": {"prod", "staging", "dev"},
			},
		},
		{
			name:  "multiple keys multiple values",
			input: "env:prod,staging;team:data,analytics",
			expected: map[string][]string{
				"env":  {"prod", "staging"},
				"team": {"data", "analytics"},
			},
		},
		{
			name:  "whitespace tolerance",
			input: " env : prod , staging ; team : data , analytics ",
			expected: map[string][]string{
				"env":  {"prod", "staging"},
				"team": {"data", "analytics"},
			},
		},
		{
			name:        "empty string",
			input:       "",
			expected:    nil,
			expectError: false,
		},
		{
			name:        "missing colon",
			input:       "env-prod",
			expectError: true,
		},
		{
			name:        "empty key",
			input:       ":prod",
			expectError: true,
		},
		{
			name:        "empty value list",
			input:       "env:",
			expectError: true,
		},
		{
			name:        "duplicate keys",
			input:       "env:prod;env:staging",
			expectError: true,
		},
		{
			name:        "empty values after trim",
			input:       "env: , , ",
			expectError: true,
		},
		{
			name:  "complex real-world example",
			input: "environment:production,staging;region:us-east-1,us-west-2;team:data",
			expected: map[string][]string{
				"environment": {"production", "staging"},
				"region":      {"us-east-1", "us-west-2"},
				"team":        {"data"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTableTagFilter(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTableTagMatching(t *testing.T) {
	tests := []struct {
		name        string
		filter      map[string][]string
		tableTags   []struct{ key, value string }
		shouldMatch bool
	}{
		{
			name: "single key matches",
			filter: map[string][]string{
				"env": {"prod"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "prod"},
			},
			shouldMatch: true,
		},
		{
			name: "single key OR match",
			filter: map[string][]string{
				"env": {"prod", "staging"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "staging"},
			},
			shouldMatch: true,
		},
		{
			name: "multiple keys AND match",
			filter: map[string][]string{
				"env":  {"prod"},
				"team": {"data"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "prod"},
				{"team", "data"},
			},
			shouldMatch: true,
		},
		{
			name: "multiple keys partial match fails",
			filter: map[string][]string{
				"env":  {"prod"},
				"team": {"data"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "prod"},
				// missing "team" tag
			},
			shouldMatch: false,
		},
		{
			name: "value mismatch",
			filter: map[string][]string{
				"env": {"prod"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "dev"},
			},
			shouldMatch: false,
		},
		{
			name: "extra table tags OK",
			filter: map[string][]string{
				"env": {"prod"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "prod"},
				{"owner", "team-a"}, // extra tag, should still match
			},
			shouldMatch: true,
		},
		{
			name: "complex AND/OR logic",
			filter: map[string][]string{
				"env":  {"prod", "staging"},
				"team": {"data", "analytics"},
			},
			tableTags: []struct{ key, value string }{
				{"env", "staging"},
				{"team", "analytics"},
				{"region", "us-east-1"}, // extra tag
			},
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate matching logic from discoverTablesByTag
			matchedTags := make(map[string]bool)

			for _, tag := range tt.tableTags {
				acceptedValues, exists := tt.filter[tag.key]
				if !exists {
					continue
				}

				if slices.Contains(acceptedValues, tag.value) {
					matchedTags[tag.key] = true
				}
			}

			matches := len(matchedTags) == len(tt.filter)
			assert.Equal(t, tt.shouldMatch, matches,
				"Filter: %v, Tags: %v, Matched: %v", tt.filter, tt.tableTags, matchedTags)
		})
	}
}
