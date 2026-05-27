// Copyright 2026 Redpanda Data, Inc.
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

// This file lives in `package host` (not `host_test`) so the benchmarks
// can reach the unexported `newRemoteBatchProcessor` + `Plugin.client`
// fields, avoiding the env / ResourceBuilder wrapper overhead and
// giving the in-process baseline a fair comparison against the gRPC
// proxy.

package host

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// benchPayload is the message body each benchmark transforms. Kept short so
// gRPC framing rather than payload size dominates the cost, which is the
// dimension we actually care about when comparing implementations.
const (
	benchPayload = "hello world this is a benchmark payload"
	benchSuffix  = "!!!"
)

// expectedTransform is what every implementation under benchmark must
// produce: "MEOW! " + UPPER(input) + suffix. Same shape as the catshout
// PoC plugin's "catshout" component.
func expectedTransform(input []byte) []byte {
	return slices.Concat([]byte("MEOW! "), bytes.ToUpper(input), []byte(benchSuffix))
}

// nativeCatshoutProcessor is the in-process reference implementation —
// what an author would write today using benthos's public/service API
// directly. Acts as the lower bound for the benchmark sweep.
type nativeCatshoutProcessor struct{ suffix []byte }

func (p *nativeCatshoutProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for _, m := range batch {
		raw, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		m.SetBytes(slices.Concat([]byte("MEOW! "), bytes.ToUpper(raw), p.suffix))
	}
	return []service.MessageBatch{batch}, nil
}

func (*nativeCatshoutProcessor) Close(context.Context) error { return nil }

// BenchmarkProcessorComparison reports per-batch ProcessBatch latency for
// three implementations of the same transform:
//
//  1. Native        — an in-process Go BatchProcessor.
//  2. PluginGo      — the catshout PoC plugin spawned via the v1 host
//     adapter (one extra subprocess hop over gRPC).
//  3. PluginPython  — the pycatshout PoC plugin (same hop, plus a
//     Python interpreter).
//
// Sub-benchmarks vary the batch size to expose per-message vs
// per-batch costs. Run with e.g.
//
//	go test -bench=. -benchmem -run=^$ ./internal/rpcplugin/v1/host/...
func BenchmarkProcessorComparison(b *testing.B) {
	cases := []struct {
		name string
		new  func(b *testing.B) (service.BatchProcessor, bool)
	}{
		{name: "Native", new: setupNativeProc},
		{name: "PluginGo", new: setupGoPluginProc},
		{name: "PluginPython", new: setupPythonPluginProc},
	}
	batchSizes := []int{1, 10, 100}

	for _, c := range cases {
		proc, ok := c.new(b)
		if !ok {
			b.Logf("skipping %s benchmark — setup unavailable", c.name)
			continue
		}
		// Sanity-check the implementation produces the expected output
		// before timing it. Catches silent regressions.
		if err := verifyTransform(b.Context(), proc); err != nil {
			b.Errorf("%s: pre-benchmark verification failed: %v", c.name, err)
			continue
		}

		for _, n := range batchSizes {
			b.Run(fmt.Sprintf("%s/batch=%d", c.name, n), func(b *testing.B) {
				ctx := b.Context()
				b.ReportAllocs()
				b.SetBytes(int64(n) * int64(len(benchPayload)))
				b.ResetTimer()
				for range b.N {
					if _, err := proc.ProcessBatch(ctx, makeBatch(n)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// verifyTransform sends a single-message batch through proc and checks the
// output matches the catshout transform.
func verifyTransform(ctx context.Context, proc service.BatchProcessor) error {
	out, err := proc.ProcessBatch(ctx, makeBatch(1))
	if err != nil {
		return err
	}
	if len(out) != 1 || len(out[0]) != 1 {
		return fmt.Errorf("unexpected output shape: %d batches, %d messages", len(out), msgCount(out))
	}
	got, err := out[0][0].AsBytes()
	if err != nil {
		return err
	}
	want := expectedTransform([]byte(benchPayload))
	if !bytes.Equal(got, want) {
		return fmt.Errorf("transform mismatch: got %q, want %q", got, want)
	}
	return nil
}

func msgCount(batches []service.MessageBatch) int {
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	return total
}

func makeBatch(n int) service.MessageBatch {
	out := make(service.MessageBatch, n)
	payload := []byte(benchPayload)
	for i := range out {
		out[i] = service.NewMessage(payload)
	}
	return out
}

// ----------------------------------------------------------------------
// per-impl setup helpers — each returns (proc, ok). ok=false means the
// dependency is missing and the case should be skipped silently.
// ----------------------------------------------------------------------

func setupNativeProc(_ *testing.B) (service.BatchProcessor, bool) {
	return &nativeCatshoutProcessor{suffix: []byte(benchSuffix)}, true
}

func setupGoPluginProc(b *testing.B) (service.BatchProcessor, bool) {
	repoRoot := repoRootForBench(b)
	dir := filepath.Join(repoRoot, "internal", "rpcplugin", "v1", "testdata", "catshout")
	if _, err := os.Stat(dir); err != nil {
		b.Logf("Go plugin testdata missing at %s: %v", dir, err)
		return nil, false
	}

	binPath := filepath.Join(b.TempDir(), "catshout-bench")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Dir = dir
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		b.Fatalf("go build catshout: %v\n%s", err, out)
	}

	plugin, proc, ok := openProc(b, []string{binPath}, "catshout")
	if !ok {
		return nil, false
	}
	_ = plugin
	return proc, true
}

func setupPythonPluginProc(b *testing.B) (service.BatchProcessor, bool) {
	if _, err := exec.LookPath("uv"); err != nil {
		b.Logf("uv not available on PATH; skipping Python plugin benchmark")
		return nil, false
	}
	repoRoot := repoRootForBench(b)
	pyDir := filepath.Join(repoRoot, "public", "plugin", "python", "v1")
	pluginMain := filepath.Join(repoRoot, "internal", "rpcplugin", "v1", "testdata", "pycatshout", "main.py")
	if _, err := os.Stat(pluginMain); err != nil {
		b.Logf("Python plugin testdata missing at %s: %v", pluginMain, err)
		return nil, false
	}
	cmd := []string{
		"uv", "run",
		"--directory", pyDir,
		"--quiet",
		"python", "-u", pluginMain,
	}
	plugin, proc, ok := openProc(b, cmd, "pycatshout")
	if !ok {
		return nil, false
	}
	_ = plugin
	return proc, true
}

// openProc spawns the supplied plugin command, opens an instance of
// componentName configured with the benchmark suffix, and returns the
// resulting remote-proxy processor. The subprocess is torn down via
// b.Cleanup.
func openProc(b *testing.B, cmd []string, componentName string) (*Plugin, service.BatchProcessor, bool) {
	env := service.NewEnvironment()
	plugin, err := RegisterPluginBinary(env, cmd,
		WithLogger(service.MockResources().Logger()),
	)
	if err != nil {
		b.Logf("registering plugin %v: %v", cmd, err)
		return nil, nil, false
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = plugin.Close(ctx)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proc, err := newRemoteBatchProcessor(ctx, plugin.client, componentName, map[string]any{
		"suffix": benchSuffix,
	})
	if err != nil {
		b.Logf("opening %s instance: %v", componentName, err)
		return nil, nil, false
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = proc.Close(ctx)
	})
	return plugin, proc, true
}

// repoRootForBench finds the connect repo root by walking up from this
// file's location.
func repoRootForBench(tb testing.TB) string {
	tb.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatalf("discovering bench file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", ".."))
}
