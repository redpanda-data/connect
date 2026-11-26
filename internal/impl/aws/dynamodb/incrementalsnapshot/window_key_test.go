// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compositeSchema() []dynamodbtypes.KeySchemaElement {
	return []dynamodbtypes.KeySchemaElement{
		{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
	}
}

func TestWindowKeyItemAndStreamAgree(t *testing.T) {
	tests := []struct {
		name   string
		item   map[string]dynamodbtypes.AttributeValue
		stream map[string]streamstypes.AttributeValue
	}{
		{
			name: "string and number",
			item: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
				"sk": &dynamodbtypes.AttributeValueMemberN{Value: "42"},
				"x":  &dynamodbtypes.AttributeValueMemberS{Value: "ignored"},
			},
			stream: map[string]streamstypes.AttributeValue{
				"pk": &streamstypes.AttributeValueMemberS{Value: "a"},
				"sk": &streamstypes.AttributeValueMemberN{Value: "42"},
			},
		},
		{
			name: "binary",
			item: map[string]dynamodbtypes.AttributeValue{
				"pk": &dynamodbtypes.AttributeValueMemberB{Value: []byte{0, 1, 2}},
				"sk": &dynamodbtypes.AttributeValueMemberS{Value: "s"},
			},
			stream: map[string]streamstypes.AttributeValue{
				"pk": &streamstypes.AttributeValueMemberB{Value: []byte{0, 1, 2}},
				"sk": &streamstypes.AttributeValueMemberS{Value: "s"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, ok := WindowKeyFromItem(tc.item, compositeSchema())
			require.True(t, ok)
			b, ok := WindowKeyFromStream(tc.stream)
			require.True(t, ok)
			assert.Equal(t, a, b)
		})
	}
}

func TestWindowKeyInjective(t *testing.T) {
	keys := []map[string]streamstypes.AttributeValue{
		{"pk": &streamstypes.AttributeValueMemberB{Value: []byte("x")}, "sk": &streamstypes.AttributeValueMemberS{Value: "1"}},
		{"pk": &streamstypes.AttributeValueMemberB{Value: []byte("y")}, "sk": &streamstypes.AttributeValueMemberS{Value: "1"}},
		{"pk": &streamstypes.AttributeValueMemberS{Value: "a;sk=b"}, "sk": &streamstypes.AttributeValueMemberS{Value: "c"}},
		{"pk": &streamstypes.AttributeValueMemberS{Value: "a"}, "sk": &streamstypes.AttributeValueMemberS{Value: "b;sk=c"}},
		{"pk": &streamstypes.AttributeValueMemberS{Value: "1"}, "sk": &streamstypes.AttributeValueMemberS{Value: "c"}},
		{"pk": &streamstypes.AttributeValueMemberN{Value: "1"}, "sk": &streamstypes.AttributeValueMemberS{Value: "c"}},
	}
	seen := map[string]int{}
	for i, k := range keys {
		s, ok := WindowKeyFromStream(k)
		require.True(t, ok)
		if j, dup := seen[s]; dup {
			t.Fatalf("keys %d and %d collide: %q", j, i, s)
		}
		seen[s] = i
	}
}

func TestWindowKeyMissingAttribute(t *testing.T) {
	_, ok := WindowKeyFromItem(map[string]dynamodbtypes.AttributeValue{
		"pk": &dynamodbtypes.AttributeValueMemberS{Value: "a"},
	}, compositeSchema())
	assert.False(t, ok)

	_, ok = WindowKeyFromStream(nil)
	assert.False(t, ok)
}
