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

package host_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/host"
)

// TestGoCatshoutEndToEnd builds the catshout testdata plugin, spawns
// it via the v1 host adapter, registers its components on a fresh
// *service.Environment, and runs a message through a benthos pipeline
// that chains catshout → reverser. Validates the four headline RFC
// ideas: in-code spec, multi-component-per-binary, byte-identical
// component interfaces, host-side discovery + proxy.
func TestGoCatshoutEndToEnd(t *testing.T) {
	binPath := buildGoPlugin(t, "catshout")

	env := service.NewEnvironment()

	plugin, err := host.RegisterPluginBinary(env, []string{binPath},
		host.WithLogger(testLogger(t)),
	)
	require.NoError(t, err, "registering plugin")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = plugin.Close(ctx)
	})

	// Both components should be advertised.
	componentNames := map[string]bool{}
	for _, c := range plugin.Components() {
		componentNames[c.Name] = true
	}
	require.True(t, componentNames["catshout"], "catshout component should be registered")
	require.True(t, componentNames["reverser"], "reverser component should be registered")

	// Build a resource pipeline that chains catshout → reverser.
	rb := env.NewResourceBuilder()
	require.NoError(t, rb.AddProcessorYAML(`
label: shouter
catshout:
  suffix: "!!!"
`))
	require.NoError(t, rb.AddProcessorYAML(`
label: flipper
reverser: {}
`))

	res, done, err := rb.Build()
	require.NoError(t, err, "building resources")
	t.Cleanup(func() {
		_ = done(t.Context())
	})

	// catshout("hello") => "MEOW! HELLO!!!"
	require.NoError(t, res.AccessProcessor(t.Context(), "shouter", func(p *service.ResourceProcessor) {
		out, err := p.Process(t.Context(), service.NewMessage([]byte("hello")))
		require.NoError(t, err)
		require.Len(t, out, 1)
		got, err := out[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "MEOW! HELLO!!!", string(got))
	}))

	// reverser("MEOW! HELLO!!!") => "!!!OLLEH !WOEM"
	require.NoError(t, res.AccessProcessor(t.Context(), "flipper", func(p *service.ResourceProcessor) {
		out, err := p.Process(t.Context(), service.NewMessage([]byte("MEOW! HELLO!!!")))
		require.NoError(t, err)
		require.Len(t, out, 1)
		got, err := out[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "!!!OLLEH !WOEM", string(got))
	}))
}

// TestPythonCatshoutEndToEnd is the Python equivalent of
// TestGoCatshoutEndToEnd. It exercises the same RFC ideas via the
// Python v1 SDK.
func TestPythonCatshoutEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("uv"); err != nil {
		t.Skip("uv not available on PATH; skipping Python end-to-end test")
	}

	repoRoot := repoRootForTest(t)
	pythonProjDir := filepath.Join(repoRoot, "public", "plugin", "python", "v1")
	pluginMain := filepath.Join(repoRoot, "internal", "rpcplugin", "v1", "testdata", "pycatshout", "main.py")
	for _, p := range []string{pythonProjDir, pluginMain} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("python testdata not present at %s: %v", p, err)
		}
	}

	env := service.NewEnvironment()

	cmd := []string{
		"uv", "run",
		"--directory", pythonProjDir,
		"--quiet",
		"python", "-u", pluginMain,
	}
	stderr := func(line string) { t.Logf("[pycatshout stderr] %s", line) }

	plugin, err := host.RegisterPluginBinary(env, cmd,
		host.WithLogger(testLogger(t)),
		host.WithStderrHook(stderr),
	)
	require.NoError(t, err, "registering python plugin")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = plugin.Close(ctx)
	})

	componentNames := map[string]bool{}
	for _, c := range plugin.Components() {
		componentNames[c.Name] = true
	}
	require.True(t, componentNames["pycatshout"], "pycatshout component should be registered")
	require.True(t, componentNames["pyreverser"], "pyreverser component should be registered")

	rb := env.NewResourceBuilder()
	require.NoError(t, rb.AddProcessorYAML(`
label: pyshouter
pycatshout:
  suffix: "!!!"
`))
	require.NoError(t, rb.AddProcessorYAML(`
label: pyflipper
pyreverser: {}
`))

	res, done, err := rb.Build()
	require.NoError(t, err, "building resources")
	t.Cleanup(func() { _ = done(t.Context()) })

	// pycatshout("hello") => "MEOW! HELLO!!!"
	require.NoError(t, res.AccessProcessor(t.Context(), "pyshouter", func(p *service.ResourceProcessor) {
		out, err := p.Process(t.Context(), service.NewMessage([]byte("hello")))
		require.NoError(t, err)
		require.Len(t, out, 1)
		got, err := out[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "MEOW! HELLO!!!", string(got))
	}))

	// pyreverser("MEOW! HELLO!!!") => "!!!OLLEH !WOEM"
	require.NoError(t, res.AccessProcessor(t.Context(), "pyflipper", func(p *service.ResourceProcessor) {
		out, err := p.Process(t.Context(), service.NewMessage([]byte("MEOW! HELLO!!!")))
		require.NoError(t, err)
		require.Len(t, out, 1)
		got, err := out[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "!!!OLLEH !WOEM", string(got))
	}))
}

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

// buildGoPlugin invokes `go build` on the named testdata subdirectory
// and returns the path to the resulting binary.
func buildGoPlugin(t *testing.T, name string) string {
	t.Helper()
	repoRoot := repoRootForTest(t)
	dir := filepath.Join(repoRoot, "internal", "rpcplugin", "v1", "testdata", name)
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("plugin source %s not available: %v", dir, err)
	}

	binPath := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = dir
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build %s: %s", dir, string(out))
	return binPath
}

// repoRootForTest finds the connect repo root by walking up from this
// test file's location.
func repoRootForTest(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "discovering test file path")
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", ".."))
}

// testLogger returns a benthos logger writing to the test's stderr,
// suitable for passing to host.WithLogger.
func testLogger(t *testing.T) *service.Logger {
	t.Helper()
	// MockResources gives us a default logger that writes to stderr —
	// good enough for tests where we just want the subprocess wrapper
	// to have something non-nil to call.
	return service.MockResources().Logger()
}
