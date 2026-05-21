// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/require"
)

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

// helpers
func amount(s string) cetypes.MetricValue {
	return cetypes.MetricValue{Amount: aws.String(s), Unit: aws.String("USD")}
}

func dailyResult(date, cost string) cetypes.ResultByTime {
	return cetypes.ResultByTime{
		TimePeriod: &cetypes.DateInterval{Start: aws.String(date), End: aws.String(date)},
		Total:      map[string]cetypes.MetricValue{"UnblendedCost": amount(cost)},
	}
}

func groupResult(usageType, cost string) cetypes.Group {
	return cetypes.Group{
		Keys: []string{usageType},
		Metrics: map[string]cetypes.MetricValue{
			"UnblendedCost": amount(cost),
		},
	}
}

func TestSummarise_AggregatesDailyTotals(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	fake := &FakeCostExplorer{
		Responses: []*costexplorer.GetCostAndUsageOutput{
			{ // totals call: daily granularity from month start through today
				ResultsByTime: []cetypes.ResultByTime{
					dailyResult("2026-05-01", "0.50"),
					dailyResult("2026-05-13", "5.00"),
					dailyResult("2026-05-14", "5.00"),
					dailyResult("2026-05-19", "3.00"),
					dailyResult("2026-05-20", "1.25"), // today
				},
			},
			{ // breakdown call: grouped by usage type, last 7 days
				ResultsByTime: []cetypes.ResultByTime{
					{
						Groups: []cetypes.Group{
							groupResult("EC2:c8g.4xlarge", "8.00"),
							groupResult("RDS:db.r6g.2xlarge", "5.50"),
						},
					},
				},
			},
		},
	}

	report, err := SummariseCosts(context.Background(), fake, "us-east-2", now)
	require.NoError(t, err)
	require.InDelta(t, 1.25, report.Today, 1e-9)
	require.InDelta(t, 8.00+1.25, report.Last7Days, 1e-9) // 2026-05-13 is exactly 7 days ago; 14..20 sum
	require.InDelta(t, 0.50+5.00+5.00+3.00+1.25, report.MonthToDate, 1e-9)
	require.Equal(t, "us-east-2", report.Region)
	require.Equal(t, "USD", report.CurrencyCode)
	require.Len(t, report.ByUsageType, 2)
}

func TestSummarise_EmptyResponseGivesZeros(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	fake := &FakeCostExplorer{
		Responses: []*costexplorer.GetCostAndUsageOutput{{}, {}},
	}
	report, err := SummariseCosts(context.Background(), fake, "us-east-2", now)
	require.NoError(t, err)
	require.Equal(t, 0.0, report.Today)
	require.Equal(t, 0.0, report.Last7Days)
	require.Equal(t, 0.0, report.MonthToDate)
	require.Empty(t, report.ByUsageType)
}

func TestSummarise_PropagatesError(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	fake := &FakeCostExplorer{
		Errs: []error{errBoom},
	}
	_, err := SummariseCosts(context.Background(), fake, "us-east-2", now)
	require.Error(t, err)
}

var errBoom = errBoomImpl("boom")

type errBoomImpl string

func (e errBoomImpl) Error() string { return string(e) }
