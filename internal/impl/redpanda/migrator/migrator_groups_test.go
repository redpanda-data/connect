// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestExtractTopics(t *testing.T) {
	tests := []struct {
		name     string
		gcos     []GroupOffset
		expected []string
	}{
		{
			name:     "empty slice",
			gcos:     []GroupOffset{},
			expected: []string{},
		},
		{
			name: "single topic single group",
			gcos: []GroupOffset{
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 0,
						At:        100,
					},
				},
			},
			expected: []string{"topic1"},
		},
		{
			name: "single topic multiple groups",
			gcos: []GroupOffset{
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 0,
						At:        100,
					},
				},
				{
					Group: "group2",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 1,
						At:        200,
					},
				},
			},
			expected: []string{"topic1"},
		},
		{
			name: "multiple topics single group",
			gcos: []GroupOffset{
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 0,
						At:        100,
					},
				},
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic2",
						Partition: 0,
						At:        200,
					},
				},
			},
			expected: []string{"topic1", "topic2"},
		},
		{
			name: "multiple topics multiple groups with duplicates",
			gcos: []GroupOffset{
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 0,
						At:        100,
					},
				},
				{
					Group: "group2",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 1,
						At:        150,
					},
				},
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic2",
						Partition: 0,
						At:        200,
					},
				},
				{
					Group: "group3",
					Offset: kadm.Offset{
						Topic:     "topic3",
						Partition: 0,
						At:        300,
					},
				},
				{
					Group: "group2",
					Offset: kadm.Offset{
						Topic:     "topic2",
						Partition: 1,
						At:        250,
					},
				},
			},
			expected: []string{"topic1", "topic2", "topic3"},
		},
		{
			name: "same topic different partitions",
			gcos: []GroupOffset{
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 0,
						At:        100,
					},
				},
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 1,
						At:        200,
					},
				},
				{
					Group: "group1",
					Offset: kadm.Offset{
						Topic:     "topic1",
						Partition: 2,
						At:        300,
					},
				},
			},
			expected: []string{"topic1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Given: GroupOffsets slice with %d entries", len(tt.gcos))

			t.Log("When: extractTopics is called")
			got := extractTopics(tt.gcos)

			t.Log("Then: unique topic names should be extracted")

			// Sort both slices for comparison since map iteration order is not guaranteed
			sort.Strings(got)
			sort.Strings(tt.expected)

			if diff := cmp.Diff(tt.expected, got); diff != "" {
				t.Errorf("extractTopics() mismatch (-want +got):\n%s", diff)
			}

			t.Logf("Got %d unique topics: %v", len(got), got)
		})
	}
}
