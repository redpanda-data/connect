// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
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

// SummariseCosts issues two CE queries against the project's tag filter, then
// folds the daily totals into today / 7d / MTD buckets and surfaces the
// usage-type breakdown verbatim. `now` is injected so tests are deterministic.
func SummariseCosts(ctx context.Context, ce CostExplorer, region string, now time.Time) (CostReport, error) {
	tagFilter := &cetypes.Expression{
		Tags: &cetypes.TagValues{
			Key:    aws.String("Project"),
			Values: []string{"redpanda-connect-bench"},
		},
	}

	// 1. Daily totals from month start through today.
	totals, err := ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{
		TimePeriod: &cetypes.DateInterval{
			Start: aws.String(monthStart(now)),
			End:   aws.String(daysAgo(now, -1)), // CE end is exclusive; +1 day captures today
		},
		Granularity: cetypes.GranularityDaily,
		Metrics:     []string{"UnblendedCost"},
		Filter:      tagFilter,
	})
	if err != nil {
		return CostReport{}, fmt.Errorf("cost explorer totals query: %w", err)
	}

	report := CostReport{Region: region, CurrencyCode: "USD"}
	todayStr := today(now)
	sevenDaysAgo := daysAgo(now, 7)
	for _, r := range totals.ResultsByTime {
		amt, _ := strconv.ParseFloat(aws.ToString(r.Total["UnblendedCost"].Amount), 64)
		report.MonthToDate += amt
		date := aws.ToString(r.TimePeriod.Start)
		if date > sevenDaysAgo {
			report.Last7Days += amt
		}
		if date == todayStr {
			report.Today += amt
		}
		// Track currency from the first non-empty row; CE returns it per row.
		if r.Total["UnblendedCost"].Unit != nil && *r.Total["UnblendedCost"].Unit != "" {
			report.CurrencyCode = *r.Total["UnblendedCost"].Unit
		}
	}

	// 2. Per-usage-type breakdown for the last 7 days.
	breakdown, err := ce.GetCostAndUsage(ctx, &costexplorer.GetCostAndUsageInput{
		TimePeriod: &cetypes.DateInterval{
			Start: aws.String(daysAgo(now, 6)),
			End:   aws.String(daysAgo(now, -1)),
		},
		Granularity: cetypes.GranularityMonthly, // single bucket — we sum manually
		Metrics:     []string{"UnblendedCost"},
		GroupBy: []cetypes.GroupDefinition{
			{Type: cetypes.GroupDefinitionTypeDimension, Key: aws.String("USAGE_TYPE")},
		},
		Filter: tagFilter,
	})
	if err != nil {
		return CostReport{}, fmt.Errorf("cost explorer breakdown query: %w", err)
	}
	for _, r := range breakdown.ResultsByTime {
		for _, g := range r.Groups {
			if len(g.Keys) == 0 {
				continue
			}
			amt, _ := strconv.ParseFloat(aws.ToString(g.Metrics["UnblendedCost"].Amount), 64)
			if amt == 0 {
				continue
			}
			report.ByUsageType = append(report.ByUsageType, UsageTypeCost{
				UsageType: g.Keys[0],
				Cost:      amt,
			})
		}
	}

	return report, nil
}

// Print writes a human-readable cost report to w. Layout is column-aligned
// and the snapshot test asserts byte-exact output, so changes here must
// update the test.
func Print(w io.Writer, r CostReport) {
	fmt.Fprintf(w, "AWS spend — Project=redpanda-connect-bench (%s)\n", r.Region)
	fmt.Fprintln(w, "Note: AWS Cost Explorer lags ~24-48h; today's spend will be partial.")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  today         $%.2f\n", r.Today)
	fmt.Fprintf(w, "  last 7 days   $%.2f\n", r.Last7Days)
	fmt.Fprintf(w, "  month-to-date $%.2f\n", r.MonthToDate)

	if len(r.ByUsageType) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "By usage type (last 7 days):")

	// Sort breakdown by cost descending for readability.
	sorted := append([]UsageTypeCost(nil), r.ByUsageType...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Cost > sorted[j].Cost })

	// Pad usage-type names so the $ column aligns.
	maxName := 0
	for _, u := range sorted {
		if len(u.UsageType) > maxName {
			maxName = len(u.UsageType)
		}
	}
	for _, u := range sorted {
		pad := maxName - len(u.UsageType)
		fmt.Fprintf(w, "  %s%s $%.2f\n", u.UsageType, padding(pad), u.Cost)
	}
}

func padding(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}

type awsCostExplorer struct {
	client *costexplorer.Client
}

// NewAWSCostExplorer builds a CostExplorer backed by the AWS SDK. Cost
// Explorer is a global service exposed only in us-east-1, regardless of
// where the project's resources live.
func NewAWSCostExplorer(ctx context.Context) (CostExplorer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		return nil, err
	}
	return &awsCostExplorer{client: costexplorer.NewFromConfig(cfg)}, nil
}

func (a *awsCostExplorer) GetCostAndUsage(ctx context.Context, in *costexplorer.GetCostAndUsageInput) (*costexplorer.GetCostAndUsageOutput, error) {
	return a.client.GetCostAndUsage(ctx, in)
}
