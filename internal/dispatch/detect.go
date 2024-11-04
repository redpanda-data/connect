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

import "context"

// CtxOnTriggerSignal creates a context that is enriched with a closure function
// which may be called by downstream components once any batch or transaction
// associated with the context has been dispatched to an output.
//
// CAVEATS:
//   - This closure may be called any number of times (or never at all)
//   - This closure is called, if ever, when a batch has been dispatched, but
//     _not delivered_. In order to detect when delivery has been successful use
//     the regular acknowledgement mechanism.
func CtxOnTriggerSignal(ctx context.Context, fn func()) context.Context {
	v, _ := ctx.Value(triggerKey).(triggerType)
	v = append(v, fn)
	return context.WithValue(ctx, triggerKey, v)
}

// TriggerSignal will call any closures associated with the provided context on
// trigger signal. This should be called by components that are able to
// distinguish between the dispatch of a message and the delivery, and should be
// called once the dispatch has occured, and is safe to call on any context any
// number of times.
func TriggerSignal(ctx context.Context) {
	v, ok := ctx.Value(triggerKey).(triggerType)
	if !ok {
		return
	}
	for _, fn := range v {
		fn()
	}
}

//------------------------------------------------------------------------------

type triggerType []func()

type triggerKeyType int

const triggerKey triggerKeyType = iota
