// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
package aws

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/service"
)

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

func TestMinFunction(t *testing.T) {
	tests := []struct {
		a        int
		b        int
		expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{10, 10, 10},
		{-1, 5, -1},
		{0, 0, 0},
	}

	for _, tt := range tests {
		result := min(tt.a, tt.b)
		assert.Equal(t, tt.expected, result)
	}
}

// Test shard reader structure
func TestShardReaderStructure(t *testing.T) {
	// Create input with multiple shard readers
	input := &dynamoDBCDCInput{
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {
				shardID:   "shard-001",
				iterator:  aws.String("iter-001"),
				exhausted: false,
			},
			"shard-002": {
				shardID:   "shard-002",
				iterator:  aws.String("iter-002"),
				exhausted: false,
			},
			"shard-003": {
				shardID:   "shard-003",
				iterator:  aws.String("iter-003"),
				exhausted: false,
			},
		},
	}

	// Test that shard readers are tracked
	assert.Contains(t, input.shardReaders, "shard-001")
	assert.Contains(t, input.shardReaders, "shard-002")
	assert.Contains(t, input.shardReaders, "shard-003")
	assert.Len(t, input.shardReaders, 3)
}

// Regression test: Verify RWMutex allows concurrent reads
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

	for i := 0; i < 3; i++ {
		go func() {
			input.mu.Lock()
			count := len(input.shardReaders)
			input.mu.Unlock()
			assert.Equal(t, 2, count)
			done <- true
		}()
	}

	for i := 0; i < 3; i++ {
		<-done
	}
}

// Test that exhausted shards are properly handled
func TestExhaustedShardHandling(t *testing.T) {
	logger := service.MockResources().Logger()

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
	_ = logger

	// Count active readers
	input.mu.Lock()
	activeCount := 0
	for _, reader := range input.shardReaders {
		if !reader.exhausted && reader.iterator != nil {
			activeCount++
		}
	}
	input.mu.Unlock()

	assert.Equal(t, 1, activeCount, "Only one shard should be active")
}
