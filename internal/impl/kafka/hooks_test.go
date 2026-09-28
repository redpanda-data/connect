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

package kafka

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestBrokerThrottleAllowsFirstThenSuppresses(t *testing.T) {
	now := time.Unix(0, 0)
	thr := newBrokerThrottle(10 * time.Second)
	thr.now = func() time.Time { return now }

	// First occurrence emits immediately.
	suppressed, ok := thr.allow(1, "read")
	require.True(t, ok)
	require.Equal(t, 0, suppressed)

	// Within the window: suppressed.
	_, ok = thr.allow(1, "read")
	require.False(t, ok)
	_, ok = thr.allow(1, "read")
	require.False(t, ok)

	// After the window: emits and reports the 2 suppressed occurrences.
	now = now.Add(10 * time.Second)
	suppressed, ok = thr.allow(1, "read")
	require.True(t, ok)
	require.Equal(t, 2, suppressed)

	// Counter resets after an emission.
	now = now.Add(10 * time.Second)
	suppressed, ok = thr.allow(1, "read")
	require.True(t, ok)
	require.Equal(t, 0, suppressed)
}

func TestBrokerThrottleIndependentKeys(t *testing.T) {
	now := time.Unix(0, 0)
	thr := newBrokerThrottle(10 * time.Second)
	thr.now = func() time.Time { return now }

	_, ok := thr.allow(1, "read")
	require.True(t, ok)
	// Different event for same broker is independent.
	_, ok = thr.allow(1, "write")
	require.True(t, ok)
	// Different broker is independent.
	_, ok = thr.allow(2, "read")
	require.True(t, ok)
}

func captureLogger() (*service.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return service.NewLoggerFromSlog(slog.New(h)), &buf
}

func TestConnHooksIgnoresNilError(t *testing.T) {
	logger, buf := captureLogger()
	h := newConnHooks(logger)

	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker-1", Port: 9092}
	h.OnBrokerConnect(meta, 0, nil, nil)
	h.OnBrokerWrite(meta, 0, 0, 0, 0, nil)
	h.OnBrokerRead(meta, 1, 0, 0, 0, nil)

	require.Empty(t, h.throttle.state, "nil errors must not touch the throttle")
	require.Empty(t, buf.String(), "nil errors must not log")
}

func TestConnHooksLogsConnectError(t *testing.T) {
	logger, buf := captureLogger()
	h := newConnHooks(logger)

	meta := kgo.BrokerMetadata{NodeID: 3, Host: "broker-3", Port: 9092}
	h.OnBrokerConnect(meta, 0, nil, errors.New("dial tcp: i/o timeout"))

	out := buf.String()
	require.Contains(t, out, "Kafka broker connection failed")
	require.Contains(t, out, "i/o timeout")
	require.Contains(t, out, "broker-3:9092")
	require.NotContains(t, out, "request", "connect has no request key")
	_, exists := h.throttle.state[throttleKey{broker: 3, event: "connect"}]
	require.True(t, exists)
}

func TestConnHooksLogsReadErrorWithRequestName(t *testing.T) {
	logger, buf := captureLogger()
	h := newConnHooks(logger)

	meta := kgo.BrokerMetadata{NodeID: 2, Host: "broker-2", Port: 9092}
	// key 1 == Fetch in the Kafka protocol.
	h.OnBrokerRead(meta, 1, 0, 0, 0, errors.New("read tcp: i/o timeout"))

	out := buf.String()
	require.Contains(t, out, "Kafka broker read failed")
	require.Contains(t, out, "Fetch")
}

func TestConnHooksThrottlesRepeatErrors(t *testing.T) {
	logger, buf := captureLogger()
	h := newConnHooks(logger)
	now := time.Unix(0, 0)
	h.throttle.now = func() time.Time { return now }

	meta := kgo.BrokerMetadata{NodeID: 5, Host: "broker-5", Port: 9092}
	for range 4 {
		h.OnBrokerRead(meta, 1, 0, 0, 0, errors.New("i/o timeout"))
	}
	require.Equal(t, 1, strings.Count(buf.String(), "Kafka broker read failed"),
		"only the first error within the window should log")

	now = now.Add(10 * time.Second)
	h.OnBrokerRead(meta, 1, 0, 0, 0, errors.New("i/o timeout"))
	require.Equal(t, 2, strings.Count(buf.String(), "Kafka broker read failed"))
	require.Contains(t, buf.String(), "suppressed")
}
