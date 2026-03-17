// Copyright 2024 Redpanda Data, Inc.
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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAddThenExtractHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers []kgo.RecordHeader
	}{
		{
			name:    "empty headers",
			headers: nil,
		},
		{
			name: "single header",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("bar")},
			},
		},
		{
			name: "multiple unique headers",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("bar")},
				{Key: "baz", Value: []byte("qux")},
			},
		},
		{
			name: "empty value",
			headers: []kgo.RecordHeader{
				{Key: "empty", Value: []byte("")},
			},
		},
		{
			name: "nil value",
			headers: []kgo.RecordHeader{
				{Key: "nil", Value: nil},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := service.NewMessage(nil)
			AddHeaders(msg, tc.headers)
			require.Equal(t, tc.headers, ExtractHeaders(msg))
		})
	}
}

func TestGetHeaderValue(t *testing.T) {
	tests := []struct {
		name    string
		headers []kgo.RecordHeader
		key     string
		want    []byte
	}{
		{
			name:    "empty headers",
			headers: nil,
			key:     "foo",
			want:    nil,
		},
		{
			name: "key found",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("bar")},
			},
			key:  "foo",
			want: []byte("bar"),
		},
		{
			name: "key not found",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("bar")},
			},
			key:  "baz",
			want: nil,
		},
		{
			name: "nil value",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: nil},
			},
			key:  "foo",
			want: nil,
		},
		{
			name: "empty value",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("")},
			},
			key:  "foo",
			want: []byte(""),
		},
		{
			name: "duplicate keys returns last",
			headers: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("first")},
				{Key: "bar", Value: []byte("middle")},
				{Key: "foo", Value: []byte("last")},
			},
			key:  "foo",
			want: []byte("last"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := GetHeaderValue(tc.headers, tc.key)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSetHeaderValue(t *testing.T) {
	tests := []struct {
		name    string
		initial []kgo.RecordHeader
		key     string
		value   []byte
		want    []kgo.RecordHeader
	}{
		{
			name:    "empty headers appends new",
			initial: nil,
			key:     "foo",
			value:   []byte("bar"),
			want: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("bar")},
			},
		},
		{
			name: "updates existing single key",
			initial: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("old")},
			},
			key:   "foo",
			value: []byte("new"),
			want: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("new")},
			},
		},
		{
			name: "updates last of duplicate keys",
			initial: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("first")},
				{Key: "bar", Value: []byte("middle")},
				{Key: "foo", Value: []byte("last")},
			},
			key:   "foo",
			value: []byte("updated"),
			want: []kgo.RecordHeader{
				{Key: "foo", Value: []byte("first")},
				{Key: "bar", Value: []byte("middle")},
				{Key: "foo", Value: []byte("updated")},
			},
		},
		{
			name: "absent key appends at end",
			initial: []kgo.RecordHeader{
				{Key: "a", Value: []byte("x")},
			},
			key:   "foo",
			value: []byte("bar"),
			want: []kgo.RecordHeader{
				{Key: "a", Value: []byte("x")},
				{Key: "foo", Value: []byte("bar")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Work on a copy to avoid mutating test table data.
			var headers []kgo.RecordHeader
			if tc.initial != nil {
				headers = make([]kgo.RecordHeader, len(tc.initial))
				copy(headers, tc.initial)
			}
			got := SetHeaderValue(headers, tc.key, tc.value)
			require.Equal(t, tc.want, got)
		})
	}
}
