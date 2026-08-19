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
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// brokerThrottle rate-limits log emissions per broker and event type. The first
// occurrence for a key is allowed immediately; further occurrences within the
// interval are suppressed and counted; the next occurrence after the interval
// is allowed and reports the suppressed count.
type brokerThrottle struct {
	interval time.Duration
	now      func() time.Time

	mu    sync.Mutex
	state map[throttleKey]*throttleEntry
}

type throttleKey struct {
	broker int32
	event  string
}

type throttleEntry struct {
	last       time.Time
	suppressed int
}

func newBrokerThrottle(interval time.Duration) *brokerThrottle {
	return &brokerThrottle{
		interval: interval,
		now:      time.Now,
		state:    map[throttleKey]*throttleEntry{},
	}
}

// allow reports whether an emission for the given broker+event should happen
// now. When ok is true, suppressed is the number of occurrences dropped since
// the previous emission.
func (t *brokerThrottle) allow(broker int32, event string) (suppressed int, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := throttleKey{broker: broker, event: event}
	now := t.now()
	e, exists := t.state[k]
	if !exists {
		t.state[k] = &throttleEntry{last: now}
		return 0, true
	}
	if now.Sub(e.last) >= t.interval {
		suppressed = e.suppressed
		e.last = now
		e.suppressed = 0
		return suppressed, true
	}
	e.suppressed++
	return 0, false
}

// connHookThrottleInterval bounds how often a given broker+event connectivity
// failure is logged during a sustained outage.
const connHookThrottleInterval = 10 * time.Second

// connHooks surfaces franz-go broker connectivity failures (connect, write, and
// read errors) at WARN so they are visible at the default log level for
// alerting, without enabling debug logging. Emissions are throttled per broker
// and event type to bound log volume during sustained outages.
type connHooks struct {
	log      *service.Logger
	throttle *brokerThrottle
}

var (
	_ kgo.HookBrokerConnect = (*connHooks)(nil)
	_ kgo.HookBrokerWrite   = (*connHooks)(nil)
	_ kgo.HookBrokerRead    = (*connHooks)(nil)
)

func newConnHooks(log *service.Logger) *connHooks {
	return &connHooks{log: log, throttle: newBrokerThrottle(connHookThrottleInterval)}
}

// OnBrokerConnect logs a WARN when a connection attempt to a broker fails (dial,
// TLS handshake, or connect timeout).
func (h *connHooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	h.emit("connect", "Kafka broker connection failed", meta, -1, err)
}

// OnBrokerWrite logs a WARN when writing a request to a broker fails.
func (h *connHooks) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	h.emit("write", "Kafka broker write failed", meta, key, err)
}

// OnBrokerRead logs a WARN when reading a response from a broker fails.
func (h *connHooks) OnBrokerRead(meta kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	h.emit("read", "Kafka broker read failed", meta, key, err)
}

// emit logs a throttled WARN for a connectivity failure. A key < 0 means the
// event has no associated Kafka request (connect).
func (h *connHooks) emit(event, msg string, meta kgo.BrokerMetadata, key int16, err error) {
	if err == nil {
		return
	}
	suppressed, ok := h.throttle.allow(meta.NodeID, event)
	if !ok {
		return
	}
	l := h.log.With(
		"broker", meta.NodeID,
		"addr", net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))),
		"err", err.Error(),
	)
	if key >= 0 {
		l = l.With("request", kmsg.NameForKey(key))
	}
	if suppressed > 0 {
		l = l.With("suppressed", suppressed)
	}
	l.Warn(msg)
}
