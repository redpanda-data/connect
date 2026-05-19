// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	minWarmup           = 2 * time.Minute
	minDuration         = 15 * time.Minute
	reservedCores       = 2
	defaultGoMemPerVCPU = 2 // GiB
)

// instanceTypeVCPU maps known EC2 instance types to their vCPU counts.
// Extend this table when new types are referenced by scenarios.
var instanceTypeVCPU = map[string]int{
	// c7i (Intel x86_64) — kept for backward compatibility with existing fixtures
	"c7i.large":    2,
	"c7i.xlarge":   4,
	"c7i.2xlarge":  8,
	"c7i.4xlarge":  16,
	"c7i.8xlarge":  32,
	"c7i.12xlarge": 48,
	"c7i.16xlarge": 64,
	// c8g (Graviton arm64) — matches arm64 AMI and arm64 Go build target
	"c8g.large":    2,
	"c8g.xlarge":   4,
	"c8g.2xlarge":  8,
	"c8g.4xlarge":  16,
	"c8g.8xlarge":  32,
	"c8g.12xlarge": 48,
	"c8g.16xlarge": 64,
}

type Scenario struct {
	Name        string         `yaml:"name"`
	Description string         `yaml:"description"`
	Connector   string         `yaml:"connector"`
	Stack       string         `yaml:"stack"`
	Infra       InfraSpec      `yaml:"infra"`
	Dataset     DatasetSpec    `yaml:"dataset"`
	Workload    *WorkloadSpec  `yaml:"workload,omitempty"`
	Pipeline    map[string]any `yaml:"pipeline"`
	Matrix      MatrixSpec     `yaml:"matrix"`
	Reset       []ResetStep    `yaml:"reset"`
}

type InfraSpec struct {
	Source map[string]any `yaml:"source"`
	Runner RunnerSpec     `yaml:"runner"`
}

type RunnerSpec struct {
	InstanceType string `yaml:"instance_type"`
}

type DatasetSpec struct {
	InitialRows       int64    `yaml:"initial_rows"`
	RowSizeBytes      int      `yaml:"row_size_bytes"`
	Tables            []string `yaml:"tables"`
	Seeder            string   `yaml:"seeder"`
	ExpectedPeakMBSec int      `yaml:"expected_peak_mb_s,omitempty"`
}

type WorkloadSpec struct {
	WriteRatePerSec int           `yaml:"write_rate_per_sec"`
	Duration        time.Duration `yaml:"duration"`
	Warmup          time.Duration `yaml:"warmup"`
}

type MatrixSpec struct {
	CPUPoints         []int                  `yaml:"cpu_points"`
	GoMemLimitPerVCPU int                    `yaml:"go_mem_limit_per_vcpu,omitempty"`
	Overrides         map[int]map[string]any `yaml:"overrides,omitempty"`
}

type ResetStep struct {
	SQL  string `yaml:"sql,omitempty"`
	Bash string `yaml:"bash,omitempty"`
}

func LoadScenario(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var s Scenario
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	if s.Matrix.GoMemLimitPerVCPU == 0 {
		s.Matrix.GoMemLimitPerVCPU = defaultGoMemPerVCPU
	}
	return &s, nil
}

func (s *Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Connector == "" {
		return fmt.Errorf("connector is required")
	}
	if s.Stack == "" {
		return fmt.Errorf("stack is required")
	}
	if len(s.Matrix.CPUPoints) == 0 {
		return fmt.Errorf("matrix.cpu_points must contain at least one value")
	}
	if s.Matrix.CPUPoints[0] < 1 {
		return fmt.Errorf("matrix.cpu_points must all be positive: %v", s.Matrix.CPUPoints)
	}
	for i := 1; i < len(s.Matrix.CPUPoints); i++ {
		if s.Matrix.CPUPoints[i] <= s.Matrix.CPUPoints[i-1] {
			return fmt.Errorf("matrix.cpu_points must be strictly ascending: %v", s.Matrix.CPUPoints)
		}
	}

	maxCPU := s.Matrix.CPUPoints[len(s.Matrix.CPUPoints)-1]
	vCPU := vcpuForInstanceType(s.Infra.Runner.InstanceType)
	if vCPU == 0 {
		return fmt.Errorf("infra.runner.instance_type %q: unknown vCPU count", s.Infra.Runner.InstanceType)
	}
	if vCPU < reservedCores+maxCPU {
		return fmt.Errorf("infra.runner.instance_type %q has %d vCPU but matrix requires %d (max sweep %d + %d reserved)",
			s.Infra.Runner.InstanceType, vCPU, reservedCores+maxCPU, maxCPU, reservedCores)
	}

	if s.Workload != nil {
		if s.Workload.Warmup < minWarmup {
			return fmt.Errorf("workload.warmup %s is below minimum %s", s.Workload.Warmup, minWarmup)
		}
		if s.Workload.Duration < minDuration {
			return fmt.Errorf("workload.duration %s is below minimum %s", s.Workload.Duration, minDuration)
		}
	} else {
		// Bounded-dataset scenario: require the size hint and verify wall-clock estimate.
		if s.Dataset.ExpectedPeakMBSec == 0 {
			return fmt.Errorf("bounded-dataset scenario must set dataset.expected_peak_mb_s")
		}
		totalBytes := s.Dataset.InitialRows * int64(s.Dataset.RowSizeBytes)
		mbTotal := totalBytes / (1024 * 1024)
		estSeconds := mbTotal / int64(s.Dataset.ExpectedPeakMBSec)
		if estSeconds < int64(minDuration.Seconds()) {
			return fmt.Errorf("bounded-dataset run would complete in %ds at %d MB/s — below minimum %s; increase dataset",
				estSeconds, s.Dataset.ExpectedPeakMBSec, minDuration)
		}
	}
	return nil
}

// vcpuForInstanceType returns the vCPU count for known instance types or 0 if
// unknown. Extend instanceTypeVCPU when new types are referenced by scenarios.
func vcpuForInstanceType(it string) int {
	return instanceTypeVCPU[it]
}
