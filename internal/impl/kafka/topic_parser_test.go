package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaTopicParsing(t *testing.T) {
	tests := []struct {
		name                    string
		defaultOffset           int64
		allowOffsets            bool
		input                   []string
		expectedTopics          []string
		expectedTopicPartitions map[string]map[int32]int64
		expectedErr             string
	}{
		{
			name:           "single topic",
			defaultOffset:  -1,
			input:          []string{"foo"},
			expectedTopics: []string{"foo"},
		},
		{
			name:           "basic topics",
			defaultOffset:  -1,
			input:          []string{"foo", "bar"},
			expectedTopics: []string{"foo", "bar"},
		},
		{
			name:           "comma separated topics",
			defaultOffset:  -1,
			input:          []string{" foo, bar ", "baz "},
			expectedTopics: []string{"foo", "bar", "baz"},
		},
		{
			name:           "partitions on topics",
			defaultOffset:  -1,
			input:          []string{"foo", "bar:1"},
			expectedTopics: []string{"foo"},
			expectedTopicPartitions: map[string]map[int32]int64{
				"bar": {
					1: -1,
				},
			},
		},
		{
			name:          "partition ranges",
			defaultOffset: -1,
			input:         []string{"foo:5-7", "bar:0-4"},
			expectedTopicPartitions: map[string]map[int32]int64{
				"foo": {5: -1, 6: -1, 7: -1},
				"bar": {0: -1, 1: -1, 2: -1, 3: -1, 4: -1},
			},
		},
		{
			name:          "offset not allowed",
			defaultOffset: -1,
			input:         []string{"foo:5:5"},
			expectedErr:   "explicit offsets are not supported by this input",
		},
		{
			name:          "offsets allowed",
			defaultOffset: -1,
			allowOffsets:  true,
			input:         []string{"foo:5:7"},
			expectedTopicPartitions: map[string]map[int32]int64{
				"foo": {5: 7},
			},
		},
		{
			name:          "offsets override",
			defaultOffset: -1,
			allowOffsets:  true,
			input:         []string{"foo:4-6:3", "foo:5:7"},
			expectedTopicPartitions: map[string]map[int32]int64{
				"foo": {4: 3, 5: 7, 6: 3},
			},
		},
		{
			name:          "offsets skip override",
			defaultOffset: -1,
			allowOffsets:  true,
			input:         []string{"foo:4-6:3", "foo:5:-1"},
			expectedTopicPartitions: map[string]map[int32]int64{
				"foo": {4: 3, 5: 3, 6: 3},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ts, tps, err := parseTopics(test.input, test.defaultOffset, test.allowOffsets)
			if test.expectedErr == "" {
				require.NoError(t, err)
				assert.Equal(t, test.expectedTopics, ts)
				assert.Equal(t, test.expectedTopicPartitions, tps)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.expectedErr)
			}
		})
	}
}
