// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"testing"
)

func TestNameConverter(t *testing.T) {
	t.Run("identical names passthrough", func(t *testing.T) {
		mappings := []TopicMapping{
			{Src: TopicInfo{Topic: "topic1"}, Dst: TopicInfo{Topic: "topic1"}},
			{Src: TopicInfo{Topic: "topic2"}, Dst: TopicInfo{Topic: "topic2"}},
		}

		conv := nameConverterFromTopicMappings(mappings)

		// Should passthrough identical names
		if got := conv.ToDst("topic1"); got != "topic1" {
			t.Errorf("ToDst(topic1) = %q, want %q", got, "topic1")
		}
		if got := conv.ToSrc("topic1"); got != "topic1" {
			t.Errorf("ToSrc(topic1) = %q, want %q", got, "topic1")
		}

		// Should handle unknown topics
		if got := conv.ToDst("unknown"); got != "unknown" {
			t.Errorf("ToDst(unknown) = %q, want %q", got, "unknown")
		}
	})

	t.Run("different names translation", func(t *testing.T) {
		mappings := []TopicMapping{
			{Src: TopicInfo{Topic: "old-topic"}, Dst: TopicInfo{Topic: "new-topic"}},
			{Src: TopicInfo{Topic: "events"}, Dst: TopicInfo{Topic: "events-v2"}},
		}

		conv := nameConverterFromTopicMappings(mappings)

		// Should translate different names
		if got := conv.ToDst("old-topic"); got != "new-topic" {
			t.Errorf("ToDst(old-topic) = %q, want %q", got, "new-topic")
		}
		if got := conv.ToSrc("new-topic"); got != "old-topic" {
			t.Errorf("ToSrc(new-topic) = %q, want %q", got, "old-topic")
		}

		if got := conv.ToDst("events"); got != "events-v2" {
			t.Errorf("ToDst(events) = %q, want %q", got, "events-v2")
		}
		if got := conv.ToSrc("events-v2"); got != "events" {
			t.Errorf("ToSrc(events-v2) = %q, want %q", got, "events")
		}
	})

	t.Run("mixed identical and different names", func(t *testing.T) {
		mappings := []TopicMapping{
			{Src: TopicInfo{Topic: "same-name"}, Dst: TopicInfo{Topic: "same-name"}},
			{Src: TopicInfo{Topic: "old-name"}, Dst: TopicInfo{Topic: "new-name"}},
		}

		conv := nameConverterFromTopicMappings(mappings)

		// Identical names should passthrough
		if got := conv.ToDst("same-name"); got != "same-name" {
			t.Errorf("ToDst(same-name) = %q, want %q", got, "same-name")
		}

		// Different names should translate
		if got := conv.ToDst("old-name"); got != "new-name" {
			t.Errorf("ToDst(old-name) = %q, want %q", got, "new-name")
		}
		if got := conv.ToSrc("new-name"); got != "old-name" {
			t.Errorf("ToSrc(new-name) = %q, want %q", got, "old-name")
		}
	})

	t.Run("empty mappings", func(t *testing.T) {
		conv := nameConverterFromTopicMappings(nil)

		// Should passthrough any name when no mappings exist
		if got := conv.ToDst("any-topic"); got != "any-topic" {
			t.Errorf("ToDst(any-topic) = %q, want %q", got, "any-topic")
		}
		if got := conv.ToSrc("any-topic"); got != "any-topic" {
			t.Errorf("ToSrc(any-topic) = %q, want %q", got, "any-topic")
		}
	})
}
