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

package mcp

import "go.opentelemetry.io/otel/propagation"

// anyMapCarrier is a TextMapCarrier that uses a map held in memory as a storage
// medium for propagated key-value pairs.
type anyMapCarrier map[string]any

// Compile time check that MapCarrier implements the TextMapCarrier.
var _ propagation.TextMapCarrier = anyMapCarrier{}

// Get returns the value associated with the passed key.
func (c anyMapCarrier) Get(key string) string {
	v := c[key]
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// Set stores the key-value pair.
func (c anyMapCarrier) Set(key, value string) {
	c[key] = value
}

// Keys lists the keys stored in this carrier.
func (c anyMapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k, v := range c {
		if _, ok := v.(string); ok {
			keys = append(keys, k)
		}
	}
	return keys
}
