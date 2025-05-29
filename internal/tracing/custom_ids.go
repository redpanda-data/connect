// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	"context"
	"math/rand"
	"sync"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type customSpanIDKeyType struct{}

var customSpanIDKey = customSpanIDKeyType{}

// WithCustomSpanID sets a custom span ID in the context.
//
// This should be used with trace.TraceProvider.Start to customize the ID of a span
func WithCustomSpanID(ctx context.Context, id trace.SpanID) context.Context {
	return context.WithValue(ctx, customSpanIDKey, id)
}

// NewIDGenerator creates a new ID generator that uses a random number.
// It is similar to the default implementation in open telemetry, except it allows
// for overriding the span ID (optionally).
func NewIDGenerator() tracesdk.IDGenerator {
	return &overridableIDGenerator{
		rand: rand.New(rand.NewSource(rand.Int63())),
	}
}

type overridableIDGenerator struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var _ tracesdk.IDGenerator = (*overridableIDGenerator)(nil)

// NewIDs implements trace.IDGenerator.
func (o *overridableIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	tid := trace.TraceID{}
	for {
		_, _ = o.rand.Read(tid[:])
		if tid.IsValid() {
			break
		}
	}
	if sid, ok := ctx.Value(customSpanIDKey).(trace.SpanID); ok {
		return tid, sid
	}
	sid := trace.SpanID{}
	for {
		_, _ = o.rand.Read(sid[:])
		if sid.IsValid() {
			break
		}
	}
	return tid, sid
}

// NewSpanID implements trace.IDGenerator.
func (o *overridableIDGenerator) NewSpanID(ctx context.Context, _ trace.TraceID) trace.SpanID {
	if id, ok := ctx.Value(customSpanIDKey).(trace.SpanID); ok {
		return id
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	sid := trace.SpanID{}
	for {
		_, _ = o.rand.Read(sid[:])
		if sid.IsValid() {
			break
		}
	}
	return sid
}
