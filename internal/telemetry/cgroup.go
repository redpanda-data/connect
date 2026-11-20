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

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cgroupRootDir = "/sys/fs/cgroup"

	cgroupV2CPUMax    = "cpu.max"
	cgroupV2MemoryMax = "memory.max"

	cgroupV1CPUQuota   = "cpu/cpu.cfs_quota_us"
	cgroupV1CPUPeriod  = "cpu/cpu.cfs_period_us"
	cgroupV1MemoryName = "memory/memory.limit_in_bytes"

	cgroupUnlimited = "max"

	millicoresPerCore = int64(1000)

	// Kernel default CFS period (microseconds), used if cpu.max omits it.
	cgroupDefaultCPUPeriodUs = int64(100000)

	// cgroup v1 reports "no limit" as a huge page-rounded value rather than a
	// sentinel string, so anything at or above this is treated as unlimited.
	cgroupV1UnlimitedThreshold = int64(1) << 62
)

// containerLimits holds the enforced container resource limits. A nil field
// means the limit is unlimited or could not be detected.
type containerLimits struct {
	// CPUMillis is the CFS quota in millicores (quota * 1000 / period), e.g.
	// 300. Integer math avoids float rounding errors.
	CPUMillis *int64
	// MemoryBytes is the cgroup memory limit.
	MemoryBytes *int64
}

// detectContainerLimits reads cgroup limits from the host's cgroup root. On
// non-Linux platforms it returns empty limits. The logger may be nil.
func detectContainerLimits(logger *service.Logger) containerLimits {
	if runtime.GOOS != "linux" {
		return containerLimits{}
	}
	return readContainerLimits(os.DirFS(cgroupRootDir), logger)
}

// readContainerLimits parses cgroup v2 files, falling back to v1 only when the
// v2 file is missing, from the given filesystem rooted at the cgroup mount.
// Errors yield absent fields, and any error other than a missing file is logged
// at debug level. The logger may be nil.
func readContainerLimits(fsys fs.FS, logger *service.Logger) containerLimits {
	var l containerLimits

	millis, err := readCPUV2(fsys)
	if errors.Is(err, fs.ErrNotExist) {
		millis, err = readCPUV1(fsys)
	}
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.With("error", err).Debug("Failed to read container CPU limit")
		}
	} else {
		l.CPUMillis = millis
	}

	mem, err := readMemoryV2(fsys)
	if errors.Is(err, fs.ErrNotExist) {
		mem, err = readMemoryV1(fsys)
	}
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.With("error", err).Debug("Failed to read container memory limit")
		}
	} else {
		l.MemoryBytes = mem
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

func readCPUV2(fsys fs.FS) (*int64, error) {
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
	return cpuMillis(quota, period)
}

func readCPUV1(fsys fs.FS) (*int64, error) {
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
	return cpuMillis(quota, period)
}

func cpuMillis(quota, period int64) (*int64, error) {
	if quota <= 0 || period <= 0 {
		return nil, fmt.Errorf("invalid cpu quota %d / period %d", quota, period)
	}
	m := quota * millicoresPerCore / period
	return &m, nil
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
