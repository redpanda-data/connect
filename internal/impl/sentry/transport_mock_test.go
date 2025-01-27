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

package sentry

import (
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
)

var argEvent = mock.AnythingOfType("*sentry.Event")

type mockTransport struct {
	mock.Mock
}

func (t *mockTransport) Flush(timeout time.Duration) bool {
	args := t.Called(timeout)

	return args.Bool(0)
}

func (t *mockTransport) Configure(options sentry.ClientOptions) {
	t.Called(options)
}

func (t *mockTransport) SendEvent(event *sentry.Event) {
	t.Called(event)
}

func (t *mockTransport) Close() {
	t.Called()
}
