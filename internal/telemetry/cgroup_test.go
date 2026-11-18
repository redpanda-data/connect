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

package telemetry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadContainerLimits(t *testing.T) {
	i := func(v int64) *int64 { return &v }

	tests := []struct {
		name    string
		files   map[string]string
		wantCPU *int64
		wantMem *int64
	}{
		{name: "empty fs"},
		{name: "v2 cpu fractional", files: map[string]string{"cpu.max": "30000 100000\n"}, wantCPU: i(300)},
		{name: "v2 cpu exact 300 millis", files: map[string]string{"cpu.max": "30000 100000"}, wantCPU: i(300)},
		{name: "v2 cpu non default period", files: map[string]string{"cpu.max": "15000 50000"}, wantCPU: i(300)},
		{name: "v2 cpu unlimited", files: map[string]string{"cpu.max": "max 100000\n"}},
		{name: "v2 cpu quota only default period", files: map[string]string{"cpu.max": "150000"}, wantCPU: i(1500)},
		{name: "v2 cpu empty", files: map[string]string{"cpu.max": ""}},
		{name: "v2 cpu non numeric quota", files: map[string]string{"cpu.max": "abc 100000"}},
		{name: "v2 cpu non numeric period", files: map[string]string{"cpu.max": "30000 abc"}},
		{name: "v2 cpu three fields", files: map[string]string{"cpu.max": "30000 100000 5"}},
		{name: "v2 cpu zero quota", files: map[string]string{"cpu.max": "0 100000"}},
		{name: "v2 cpu negative quota", files: map[string]string{"cpu.max": "-5 100000"}},
		{name: "v2 cpu zero period", files: map[string]string{"cpu.max": "30000 0"}},
		{name: "v2 cpu negative period", files: map[string]string{"cpu.max": "30000 -1"}},

		{name: "v2 memory bytes", files: map[string]string{"memory.max": "1200000000"}, wantMem: i(1200000000)},
		{name: "v2 memory trailing newline", files: map[string]string{"memory.max": "1200000000\n"}, wantMem: i(1200000000)},
		{name: "v2 memory max", files: map[string]string{"memory.max": "max\n"}},
		{name: "v2 memory zero", files: map[string]string{"memory.max": "0"}},
		{name: "v2 memory negative", files: map[string]string{"memory.max": "-1"}},
		{name: "v2 memory garbage", files: map[string]string{"memory.max": "lots"}},
		{name: "v2 memory empty", files: map[string]string{"memory.max": ""}},

		{
			name: "v1 cpu half core",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":  "50000\n",
				"cpu/cpu.cfs_period_us": "100000\n",
			},
			wantCPU: i(500),
		},
		{
			name: "v1 cpu unlimited",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":  "-1",
				"cpu/cpu.cfs_period_us": "100000",
			},
		},
		{
			name:  "v1 cpu missing period",
			files: map[string]string{"cpu/cpu.cfs_quota_us": "50000"},
		},
		{
			name: "v1 cpu garbage quota",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":  "x",
				"cpu/cpu.cfs_period_us": "100000",
			},
		},
		{
			name: "v1 cpu zero quota",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":  "0",
				"cpu/cpu.cfs_period_us": "100000",
			},
		},
		{
			name: "v1 cpu zero period",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":  "50000",
				"cpu/cpu.cfs_period_us": "0",
			},
		},
		{name: "v1 memory bytes", files: map[string]string{"memory/memory.limit_in_bytes": "400000000\n"}, wantMem: i(400000000)},
		{name: "v1 memory unlimited sentinel", files: map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712"}},
		{name: "v1 memory garbage", files: map[string]string{"memory/memory.limit_in_bytes": "nope"}},
		{name: "v1 memory zero", files: map[string]string{"memory/memory.limit_in_bytes": "0"}},

		{
			name: "malformed v2 cpu does not fall back to valid v1",
			files: map[string]string{
				"cpu.max":               "garbage",
				"cpu/cpu.cfs_quota_us":  "50000",
				"cpu/cpu.cfs_period_us": "100000",
			},
		},
		{
			name: "unlimited v2 cpu does not fall back to v1",
			files: map[string]string{
				"cpu.max":               "max 100000",
				"cpu/cpu.cfs_quota_us":  "50000",
				"cpu/cpu.cfs_period_us": "100000",
			},
		},
		{
			name: "malformed v2 memory does not fall back to valid v1",
			files: map[string]string{
				"memory.max":                   "garbage",
				"memory/memory.limit_in_bytes": "400000000",
			},
		},
		{
			name: "unlimited v2 memory does not fall back to v1",
			files: map[string]string{
				"memory.max":                   "max",
				"memory/memory.limit_in_bytes": "400000000",
			},
		},
		{
			name: "v2 wins over v1 when both valid",
			files: map[string]string{
				"cpu.max":                      "20000 100000",
				"memory.max":                   "100",
				"cpu/cpu.cfs_quota_us":         "50000",
				"cpu/cpu.cfs_period_us":        "100000",
				"memory/memory.limit_in_bytes": "400000000",
			},
			wantCPU: i(200),
			wantMem: i(100),
		},
		{
			name: "v2 missing uses v1 for both",
			files: map[string]string{
				"cpu/cpu.cfs_quota_us":         "50000",
				"cpu/cpu.cfs_period_us":        "100000",
				"memory/memory.limit_in_bytes": "400000000",
			},
			wantCPU: i(500),
			wantMem: i(400000000),
		},
		{
			name: "v2 cpu with v1 memory",
			files: map[string]string{
				"cpu.max":                      "30000 100000",
				"memory/memory.limit_in_bytes": "400000000",
			},
			wantCPU: i(300),
			wantMem: i(400000000),
		},
		{
			name: "v1 cpu with v2 memory",
			files: map[string]string{
				"memory.max":            "1200000000",
				"cpu/cpu.cfs_quota_us":  "50000",
				"cpu/cpu.cfs_period_us": "100000",
			},
			wantCPU: i(500),
			wantMem: i(1200000000),
		},
		{
			name: "malformed v2 cpu with valid v2 memory",
			files: map[string]string{
				"cpu.max":    "garbage",
				"memory.max": "1200000000",
			},
			wantMem: i(1200000000),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fsys := fstest.MapFS{}
			for name, content := range test.files {
				fsys[name] = &fstest.MapFile{Data: []byte(content)}
			}

			got := readContainerLimits(fsys, nil)

			if test.wantCPU == nil {
				assert.Nil(t, got.CPUMillis)
			} else if assert.NotNil(t, got.CPUMillis) {
				assert.Equal(t, *test.wantCPU, *got.CPUMillis)
			}
			if test.wantMem == nil {
				assert.Nil(t, got.MemoryBytes)
			} else if assert.NotNil(t, got.MemoryBytes) {
				assert.Equal(t, *test.wantMem, *got.MemoryBytes)
			}
		})
	}
}

func TestHostInfoLimitsJSON(t *testing.T) {
	cpu := int64(300)
	mem := int64(1200000000)

	tests := []struct {
		name     string
		host     hostInfo
		wantCPU  any
		wantMem  any
		wantKeys bool
	}{
		{name: "nil limits omitted", host: hostInfo{GoOS: "linux"}},
		{
			name:     "limits present",
			host:     hostInfo{GoOS: "linux", CPULimitMillis: &cpu, MemoryLimitBytes: &mem},
			wantCPU:  float64(300),
			wantMem:  float64(1200000000),
			wantKeys: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, err := json.Marshal(test.host)
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(b, &m))

			if !test.wantKeys {
				assert.NotContains(t, m, "cpuLimitMillis")
				assert.NotContains(t, m, "cpuLimit")
				assert.NotContains(t, m, "memoryLimitBytes")
				return
			}
			assert.Equal(t, test.wantCPU, m["cpuLimitMillis"])
			assert.NotContains(t, m, "cpuLimit")
			assert.Equal(t, test.wantMem, m["memoryLimitBytes"])
		})
	}
}

func TestReadContainerLimitsSymlinkedV1Layout(t *testing.T) {
	dir := t.TempDir()

	write := func(name, content string) {
		p := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
	write("cpu,cpuacct/cpu.cfs_quota_us", "50000\n")
	write("cpu,cpuacct/cpu.cfs_period_us", "100000\n")
	write("memory/memory.limit_in_bytes", "400000000\n")

	// AL2 layout: cpu is a symlink to the co-mounted cpu,cpuacct controller.
	if err := os.Symlink("cpu,cpuacct", filepath.Join(dir, "cpu")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	got := readContainerLimits(os.DirFS(dir), nil)

	require.NotNil(t, got.CPUMillis)
	assert.Equal(t, int64(500), *got.CPUMillis)
	require.NotNil(t, got.MemoryBytes)
	assert.Equal(t, int64(400000000), *got.MemoryBytes)
}
