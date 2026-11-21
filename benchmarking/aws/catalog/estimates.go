// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package catalog

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

// defaultEstimatesJSON holds starting values for a 4-point sweep's cost and
// duration. The Connect PM owns the file.
//
//go:embed estimates.json
var defaultEstimatesJSON []byte

// Estimate is the expected cost and duration of one 4-point bench sweep. It
// excludes queue wait.
type Estimate struct {
	AWSUSD  float64 `json:"aws_usd"`
	SaaSUSD float64 `json:"saas_usd"`
	Minutes int     `json:"minutes"`
}

type estimatesConfig struct {
	Stacks map[string]struct {
		USD     float64 `json:"usd"`
		Minutes int     `json:"minutes"`
	} `json:"stacks"`
	Sinks map[string]struct {
		USD               float64            `json:"usd"`
		USDPerXSmallSweep float64            `json:"usd_per_xsmall_sweep"`
		SizeMultiplier    map[string]float64 `json:"size_multiplier"`
	} `json:"sinks"`
}

func loadEstimates(b []byte) (estimatesConfig, error) {
	var cfg estimatesConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return estimatesConfig{}, fmt.Errorf("parse estimates: %w", err)
	}
	return cfg, nil
}

// EstimateRun estimates a sweep on stack ("postgres", "mysql", "none") into
// sink (a plan sink kind). sinkSize is the warehouse size and is required
// for sinks priced by size (snowflake) and rejected for the others.
func EstimateRun(stack, sink, sinkSize string) (Estimate, error) {
	cfg, err := loadEstimates(defaultEstimatesJSON)
	if err != nil {
		return Estimate{}, err
	}
	st, ok := cfg.Stacks[stack]
	if !ok {
		return Estimate{}, fmt.Errorf("no estimate for stack %q", stack)
	}
	sk, ok := cfg.Sinks[sink]
	if !ok {
		return Estimate{}, fmt.Errorf("no estimate for sink %q", sink)
	}
	e := Estimate{AWSUSD: st.USD, Minutes: st.Minutes, SaaSUSD: sk.USD}
	if sk.SizeMultiplier == nil {
		if sinkSize != "" {
			return Estimate{}, fmt.Errorf("sink %q takes no size, got %q", sink, sinkSize)
		}
		return e, nil
	}
	m, ok := sk.SizeMultiplier[strings.ToUpper(sinkSize)]
	if !ok {
		return Estimate{}, fmt.Errorf("no estimate for %s size %q", sink, sinkSize)
	}
	e.SaaSUSD = sk.USDPerXSmallSweep * m
	return e, nil
}
