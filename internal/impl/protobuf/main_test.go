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

package protobuf

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain verifies that no goroutines are leaked by the tests in this
// package (CON-179 R2).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreCurrent(),
		// github.com/bufbuild/prototransform leaves its SchemaWatcher poll
		// loop running: protobufProc.Close is a no-op and MultiModuleWatcher
		// exposes no stop hook, so watchers started by BSR-backed tests live
		// for the remainder of the process.
		goleak.IgnoreTopFunction("github.com/bufbuild/prototransform.(*SchemaWatcher).start.func1"),
		// net/http keepalive connections owned by the HTTP client of the
		// unstoppable SchemaWatcher above; they linger until the transport is
		// garbage collected.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
