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

package confluent

import "testing"

// benchCoerce builds the coercer once (as the transcoder does per schema) and
// measures only the per-message coercion cost.
func benchCoerce(b *testing.B, schema string, refs map[string]string, msg string) {
	b.Helper()
	c, err := newJSONSchemaCoercer(schema, refs)
	if err != nil {
		b.Fatal(err)
	}
	data := []byte(msg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.coerceMessage(data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCoerceFlatObject is the common case: a flat object of scalars (the
// Old Mutual Payment shape).
func BenchmarkCoerceFlatObject(b *testing.B) {
	schema := `{"type":"object","properties":{"payment_id":{"type":"string"},"amount_cents":{"type":"integer"},"currency":{"type":"string"},"initiated_at":{"type":"integer"}}}`
	msg := `{"payment_id":"p-1","amount_cents":123456789012345,"currency":"ZAR","initiated_at":1777990438791}`
	benchCoerce(b, schema, nil, msg)
}

// BenchmarkCoerceOneOf exercises the union path, where each branch schema is
// compiled per message by validatesAgainst.
func BenchmarkCoerceOneOf(b *testing.B) {
	schema := `{"type":"object","properties":{"v":{"oneOf":[{"type":"integer"},{"type":"string"}]}}}`
	msg := `{"v":5}`
	benchCoerce(b, schema, nil, msg)
}
