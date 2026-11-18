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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	cgroupRootDir = "/sys/fs/cgroup"

	cgroupV2CPUMax    = "cpu.max"
	cgroupV2MemoryMax = "memory.max"

	cgroupV1CPUQuota   = "cpu/cpu.cfs_quota_us"
	cgroupV1CPUPeriod  = "cpu/cpu.cfs_period_us"
	cgroupV1MemoryName = "memory/memory.limit_in_bytes"

	cgroupUnlimited = "max"

	// Kernel default CFS period (microseconds), used if cpu.max omits it.
	cgroupDefaultCPUPeriodUs = int64(100000)

	// cgroup v1 reports "no limit" as a huge page-rounded value rather than a
	// sentinel string, so anything at or above this is treated as unlimited.
	cgroupV1UnlimitedThreshold = int64(1) << 62
)

// containerLimits holds the enforced container resource limits. A nil field
// means the limit is unlimited or could not be detected.
type containerLimits struct {
	// CPUCores is the CFS quota divided by the period, e.g. 0.3.
	CPUCores *float64
	// MemoryBytes is the cgroup memory limit.
	MemoryBytes *int64
}

// detectContainerLimits reads cgroup limits from the host's cgroup root. On
// non-Linux platforms it returns empty limits.
func detectContainerLimits() containerLimits {
	if runtime.GOOS != "linux" {
		return containerLimits{}
	}
	return readContainerLimits(os.DirFS(cgroupRootDir))
}

// readContainerLimits parses cgroup v2 files, falling back to v1, from the
// given filesystem rooted at the cgroup mount. Errors yield absent fields.
func readContainerLimits(fsys fs.FS) containerLimits {
	var l containerLimits

	if cores, err := readCPUV2(fsys); err == nil {
		l.CPUCores = cores
	} else if errors.Is(err, fs.ErrNotExist) {
		if cores, err := readCPUV1(fsys); err == nil {
			l.CPUCores = cores
		}
	}

	if mem, err := readMemoryV2(fsys); err == nil {
		l.MemoryBytes = mem
	} else if errors.Is(err, fs.ErrNotExist) {
		if mem, err := readMemoryV1(fsys); err == nil {
			l.MemoryBytes = mem
		}
	}
	return l
}

func readTrimmed(fsys fs.FS, name string) (string, error) {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func readCPUV2(fsys fs.FS) (*float64, error) {
	s, err := readTrimmed(fsys, cgroupV2CPUMax)
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(s)
	if len(fields) < 1 || len(fields) > 2 {
		return nil, fmt.Errorf("unexpected cpu.max format: %q", s)
	}
	if fields[0] == cgroupUnlimited {
		return nil, nil
	}
	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing cpu.max quota: %w", err)
	}
	period := cgroupDefaultCPUPeriodUs
	if len(fields) == 2 {
		if period, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
			return nil, fmt.Errorf("parsing cpu.max period: %w", err)
		}
	}
	return cpuCores(quota, period)
}

func readCPUV1(fsys fs.FS) (*float64, error) {
	qs, err := readTrimmed(fsys, cgroupV1CPUQuota)
	if err != nil {
		return nil, err
	}
	ps, err := readTrimmed(fsys, cgroupV1CPUPeriod)
	if err != nil {
		return nil, err
	}
	quota, err := strconv.ParseInt(qs, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing cfs_quota_us: %w", err)
	}
	period, err := strconv.ParseInt(ps, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing cfs_period_us: %w", err)
	}
	if quota < 0 {
		return nil, nil
	}
	return cpuCores(quota, period)
}

func cpuCores(quota, period int64) (*float64, error) {
	if quota <= 0 || period <= 0 {
		return nil, fmt.Errorf("invalid cpu quota %d / period %d", quota, period)
	}
	c := float64(quota) / float64(period)
	return &c, nil
}

func readMemoryV2(fsys fs.FS) (*int64, error) {
	s, err := readTrimmed(fsys, cgroupV2MemoryMax)
	if err != nil {
		return nil, err
	}
	if s == cgroupUnlimited {
		return nil, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing memory.max: %w", err)
	}
	if v <= 0 {
		return nil, fmt.Errorf("invalid memory.max %d", v)
	}
	return &v, nil
}

func readMemoryV1(fsys fs.FS) (*int64, error) {
	s, err := readTrimmed(fsys, cgroupV1MemoryName)
	if err != nil {
		return nil, err
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parsing memory.limit_in_bytes: %w", err)
	}
	if v <= 0 || v >= cgroupV1UnlimitedThreshold {
		return nil, nil
	}
	return &v, nil
}
