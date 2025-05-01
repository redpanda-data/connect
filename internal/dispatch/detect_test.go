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

package dispatch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDispatchNA(t *testing.T) {
	// Just ensures we don't panic

	ctx := t.Context()
	TriggerSignal(ctx)
	TriggerSignal(ctx)
	TriggerSignal(ctx)

	ctx = t.Context()
	TriggerSignal(ctx)
	TriggerSignal(ctx)
	TriggerSignal(ctx)

	type fooKeyType int
	var fooKey fooKeyType

	ctx = context.WithValue(ctx, fooKey, "bar")
	TriggerSignal(ctx)
	TriggerSignal(ctx)
	TriggerSignal(ctx)
}

func TestDispatchHappy(t *testing.T) {
	seen := []string{}

	ctx := t.Context()
	ctx = CtxOnTriggerSignal(ctx, func() {
		seen = append(seen, "root")
	})

	actx := CtxOnTriggerSignal(ctx, func() {
		seen = append(seen, "a")
	})

	bctx := CtxOnTriggerSignal(ctx, func() {
		seen = append(seen, "b")
	})

	cctx := CtxOnTriggerSignal(actx, func() {
		seen = append(seen, "c")
	})

	TriggerSignal(actx)
	TriggerSignal(actx)
	TriggerSignal(bctx)
	TriggerSignal(cctx)

	assert.Equal(t, []string{
		"root", "a",
		"root", "a",
		"root", "b",
		"root", "a", "c",
	}, seen)
}
