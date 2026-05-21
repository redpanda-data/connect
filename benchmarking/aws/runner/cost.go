// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
)

// CostExplorer is the narrow slice of AWS Cost Explorer that cost-check
// uses. Tests fake this; production wires an awsCostExplorer that calls the
// real SDK.
type CostExplorer interface {
	GetCostAndUsage(ctx context.Context, in *costexplorer.GetCostAndUsageInput) (*costexplorer.GetCostAndUsageOutput, error)
}

// CostReport is the structured output of cost-check, ready to print.
type CostReport struct {
	Region       string
	Today        float64
	Last7Days    float64
	MonthToDate  float64
	ByUsageType  []UsageTypeCost
	CurrencyCode string // "USD" in the bench account; surfaced for clarity
}

// UsageTypeCost is one bucketed line in the breakdown table.
type UsageTypeCost struct {
	UsageType string
	Cost      float64
}

// FakeCostExplorer returns canned CE responses for tests, keyed by call
// order. Tests construct an instance with the responses they want and the
// fake serves them in sequence.
type FakeCostExplorer struct {
	Responses []*costexplorer.GetCostAndUsageOutput
	Errs      []error
	calls     int
}

func (f *FakeCostExplorer) GetCostAndUsage(_ context.Context, _ *costexplorer.GetCostAndUsageInput) (*costexplorer.GetCostAndUsageOutput, error) {
	i := f.calls
	f.calls++
	if i < len(f.Errs) && f.Errs[i] != nil {
		return nil, f.Errs[i]
	}
	if i < len(f.Responses) {
		return f.Responses[i], nil
	}
	return &costexplorer.GetCostAndUsageOutput{}, nil
}

// today returns YYYY-MM-DD for the given t in UTC.
func today(t time.Time) string { return t.UTC().Format("2006-01-02") }

// monthStart returns the first day of t's UTC month formatted YYYY-MM-DD.
func monthStart(t time.Time) string {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

// daysAgo returns t - n days formatted YYYY-MM-DD UTC.
func daysAgo(t time.Time, n int) string {
	return t.UTC().AddDate(0, 0, -n).Format("2006-01-02")
}
