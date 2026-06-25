// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/confx"
)

func TestFilterSalesforceTopics(t *testing.T) {
	re := func(patterns ...string) []*regexp.Regexp {
		var out []*regexp.Regexp
		for _, p := range patterns {
			out = append(out, regexp.MustCompile(p))
		}
		return out
	}

	// A mix of bare sObjects, an explicit CDC channel, the firehose, and a
	// Platform Event topic. The firehose and PE topics carry no single sObject.
	topics := []string{"Account", "Contact", "/data/OrderChangeEvent", "/data/ChangeEvents", "/event/Sync__e"}

	tests := []struct {
		name    string
		include []*regexp.Regexp
		exclude []*regexp.Regexp
		want    []string
	}{
		{
			name: "no patterns returns all",
			want: topics,
		},
		{
			name:    "include only keeps matching sObjects plus non-sObject topics",
			include: re(`^Account$`, `^Order$`),
			want:    []string{"Account", "/data/OrderChangeEvent", "/data/ChangeEvents", "/event/Sync__e"},
		},
		{
			name:    "exclude only drops matching sObjects",
			exclude: re(`^Contact$`),
			want:    []string{"Account", "/data/OrderChangeEvent", "/data/ChangeEvents", "/event/Sync__e"},
		},
		{
			name:    "combined include and exclude (exclude wins)",
			include: re(`^Account$`, `^Contact$`),
			exclude: re(`^Contact$`),
			want:    []string{"Account", "/data/ChangeEvents", "/event/Sync__e"},
		},
		{
			name:    "include matches no sObject keeps only non-sObject topics",
			include: re(`^Nonexistent$`),
			want:    []string{"/data/ChangeEvents", "/event/Sync__e"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filter := &confx.RegexpFilter{Include: tc.include, Exclude: tc.exclude}
			got, err := filterSalesforceTopics(filter, topics)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
