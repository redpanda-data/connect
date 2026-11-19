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

package servicetest_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/servicetest"
)

// recordingTB records failures instead of failing the real test. FailNow
// stops the calling goroutine, like testing.T does.
type recordingTB struct {
	testing.TB
	errors []string
	failed bool
}

func (*recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingTB) FailNow() {
	r.failed = true
	runtime.Goexit()
}

// runStoppable calls fn in a new goroutine so that recordingTB.FailNow can
// stop it without stopping the real test.
func runStoppable(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func TestNewInterpolatedStringValid(t *testing.T) {
	is := servicetest.NewInterpolatedString(t, `foo-${! meta("bar") }`)
	require.NotNil(t, is)

	msg := service.NewMessage(nil)
	msg.MetaSetMut("bar", "baz")
	got, err := is.TryString(msg)
	require.NoError(t, err)
	assert.Equal(t, "foo-baz", got)
}

func TestNewInterpolatedStringInvalidFailsTest(t *testing.T) {
	rec := &recordingTB{TB: t}
	reached := false
	runStoppable(func() {
		servicetest.NewInterpolatedString(rec, `${! meta("bar" }`)
		reached = true
	})

	assert.True(t, rec.failed, "expected the test to fail")
	assert.False(t, reached, "expected the test to stop")
	require.Len(t, rec.errors, 1)
	assert.Contains(t, rec.errors[0], "expected")
}
