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

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
)

// warningCapture is a slog handler keeping the message of every warning.
type warningCapture struct {
	mu       sync.Mutex
	warnings []string
}

func (*warningCapture) Enabled(context.Context, slog.Level) bool { return true }

func (w *warningCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level != slog.LevelWarn {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warnings = append(w.warnings, r.Message)
	return nil
}

func (w *warningCapture) WithAttrs([]slog.Attr) slog.Handler { return w }

func (w *warningCapture) WithGroup(string) slog.Handler { return w }

func (w *warningCapture) find(substr string) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range w.warnings {
		if strings.Contains(m, substr) {
			return m, true
		}
	}
	return "", false
}

// TestIntegrationRedpandaInputNackWarning checks that a record the output
// keeps rejecting, here through reject_errored after a failing processor, is
// reported by the redpanda input instead of being replayed in silence.
func TestIntegrationRedpandaInputNackWarning(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	t.Log("Given: a running Redpanda instance with one record in the topic")
	rpe, err := redpandatest.StartRedpanda(t, true)
	require.NoError(t, err)

	topic := "nack-warning"
	produceMessages(t, rpe, topic, `{"foo":"bar"}`, 0, 1, false, 0)

	logs := &warningCapture{}
	sb := service.NewStreamBuilder()
	sb.SetLogger(slog.New(logs))
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: nack-warning

pipeline:
  processors:
    - mapping: 'root = throw("always fails")'

output:
  reject_errored:
    drop: {}
`, rpe.BrokerAddr, topic)))

	stream, err := sb.Build()
	require.NoError(t, err)

	t.Log("When: the stream fails every record and reject_errored rejects it")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = stream.StopWithin(10 * time.Second)
		<-done
	})

	t.Log("Then: the input logs a warning naming the rejected record")
	var warning string
	require.Eventually(t, func() bool {
		var ok bool
		warning, ok = logs.find("Output rejected 1 message(s) of topic " + topic + " partition 0 starting at offset 0")
		return ok
	}, 30*time.Second, 100*time.Millisecond)
	assert.Contains(t, warning, "consecutive rejection(s) on this partition")
	assert.Contains(t, warning, "replayed until they are delivered")
	assert.Contains(t, warning, "always fails")
}
