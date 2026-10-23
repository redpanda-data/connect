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

package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestExtJSONFromMapTimestampIsDateTime covers redpanda-data/connect#2557: a
// Bloblang timestamp value must be converted to a BSON DateTime (ISODate) when
// writing to MongoDB rather than a plain string. A plain JSON string is left
// untouched, as it carries no BSON type information.
func TestExtJSONFromMapTimestampIsDateTime(t *testing.T) {
	mapping, err := bloblang.Parse(`root = {"ts": this.ts, "s": "x"}`)
	if err != nil {
		t.Fatal(err)
	}
	// A Bloblang timestamp literal (`this.ts.ts_parse(...)`, `timestamp(...)`,
	// etc.) is carried as a Go time.Time in the structured message.
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"ts": time.Date(2024, 5, 1, 8, 59, 7, 0, time.UTC),
	})
	batch := service.MessageBatch{msg}
	exec := batch.BloblangExecutor(mapping)

	got, err := extJSONFromMap(0, exec)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("extJSONFromMap returned %T => %#v", got, got)

	raw, err := bson.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, e := range doc {
		t.Logf("key=%q type=%T value=%v", e.Key, e.Value, e.Value)
	}

	var found any
	for _, e := range doc {
		if e.Key == "ts" {
			found = e.Value
		}
	}
	if found == nil {
		t.Fatal("no ts field in document")
	}
	if _, ok := found.(bson.DateTime); !ok {
		t.Fatalf("BUG #2557: ts persisted as %T (%v), want bson.DateTime (ISODate)", found, found)
	}

	want := time.Date(2024, 5, 1, 8, 59, 7, 0, time.UTC)
	if got := found.(bson.DateTime).Time(); !got.Equal(want) {
		t.Fatalf("timestamp round-tripped to %v, want %v", got, want)
	}
}

// TestExtJSONFromMapPlainStringStaysString ensures a plain JSON string that
// merely looks like a date is left as a string: it carries no BSON type
// information and must not be silently coerced into a DateTime.
func TestExtJSONFromMapPlainStringStaysString(t *testing.T) {
	mapping, err := bloblang.Parse(`root = {"ts": this.ts}`)
	if err != nil {
		t.Fatal(err)
	}
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{"ts": "2024-05-01T08:59:07Z"})
	batch := service.MessageBatch{msg}
	exec := batch.BloblangExecutor(mapping)

	got, err := extJSONFromMap(0, exec)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, e := range doc {
		if e.Key != "ts" {
			continue
		}
		if _, ok := e.Value.(string); !ok {
			t.Fatalf("ts became %T (%v), want string", e.Value, e.Value)
		}
		return
	}
	t.Fatal("no ts field in document")
}
