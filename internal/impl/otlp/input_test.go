// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const opTimeout = 5 * time.Second

// testInput is a unified helper function to test inputs with different signal
// types and protocols.
func testInput(
	t *testing.T,
	address string,
	signalType otlp.SignalType,
	exportFn func(ctx context.Context, address string) error,
	validateFn func(t *testing.T, msgBytes []byte),
	inputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	inputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
) {
	t.Helper()

	yamlConfig := fmt.Sprintf(`address: "%s"
encoding: protobuf`, address)
	input := startInput(t, inputSpec, inputCtor, yamlConfig)

	received := make(chan service.MessageBatch, 1)
	readErr := make(chan error, 1)
	go func() {
		batch, aFn, err := input.ReadBatch(t.Context())
		aFn(t.Context(), nil) //nolint:errcheck

		if err != nil {
			readErr <- err
		} else {
			received <- batch
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// Export data
	require.NoError(t, exportFn(t.Context(), address))

	// Wait for message
	var batch service.MessageBatch
	select {
	case batch = <-received:
		// continue
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(opTimeout):
		t.Fatal("Timeout waiting for message")
	}

	// Assert batch content - expect protobuf messages
	require.NotEmpty(t, batch)

	// Validate each message
	for _, msg := range batch {
		// Check signal type metadata
		s, ok := msg.MetaGet(otlp.MetadataKeySignalType)
		require.True(t, ok)
		require.Equal(t, signalType.String(), s)

		// Unmarshal and validate message content
		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		validateFn(t, msgBytes)
	}
}

// startInput is a helper that creates, connects, and returns an input with cleanup.
func startInput(
	t *testing.T,
	inputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	inputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
	yamlConfig string,
	opts ...func(*service.Resources),
) service.BatchInput {
	t.Helper()

	pConf, err := inputSpec.ParseYAML(yamlConfig, nil)
	require.NoError(t, err)

	res := service.MockResources()
	license.InjectTestService(res)
	input, err := inputCtor(pConf, res)
	require.NoError(t, err)

	for _, opt := range opts {
		opt(res)
	}

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	return input
}
