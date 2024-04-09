package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamIDParser(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		remaining   string
		shard       string
		errContains string
	}{
		{
			name:      "no shards stream name",
			id:        "foo-bar",
			remaining: "foo-bar",
		},
		{
			name:      "no shards stream arn",
			id:        "arn:aws:kinesis:region:account-id:stream/stream-name",
			remaining: "arn:aws:kinesis:region:account-id:stream/stream-name",
		},
		{
			name:      "sharded stream name",
			id:        "foo-bar:baz",
			remaining: "foo-bar",
			shard:     "baz",
		},
		{
			name:      "sharded stream arn",
			id:        "arn:aws:kinesis:region:account-id:stream/stream-name:baz",
			remaining: "arn:aws:kinesis:region:account-id:stream/stream-name",
			shard:     "baz",
		},
		{
			name:        "multiple shards stream name",
			id:          "foo-bar:baz:buz",
			errContains: "only one shard should be specified",
		},
		{
			name:        "multiple shards stream arn",
			id:          "arn:aws:kinesis:region:account-id:stream/stream-name:baz:buz",
			errContains: "only one shard should be specified",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			rem, shard, err := parseStreamID(test.id)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.remaining, rem)
				assert.Equal(t, test.shard, shard)
			}
		})
	}
}
