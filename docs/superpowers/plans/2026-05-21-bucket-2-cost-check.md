# Bucket 2 — cost-check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `runner cost-check` (CLI + `task aws:cost-check`) that queries AWS Cost Explorer filtered on `Project=redpanda-connect-bench` and prints today, last-7-days, and month-to-date spend plus a per-usage-type breakdown. Replaces the existing stub in `main.go`.

**Architecture:** A new `cost.go` in the runner package defines a narrow `CostExplorer` interface (so tests stay AWS-free via a `FakeCostExplorer`), an `awsCostExplorer` impl using `aws-sdk-go-v2`, a `Summarise` function that aggregates SDK responses into a `CostReport` struct, and a `Print` function with deterministic column alignment. The CLI replaces the existing stub at `main.go:357`.

**Tech Stack:** Go, `github.com/aws/aws-sdk-go-v2/service/costexplorer`, `testify` for tests.

**Spec:** [`docs/superpowers/specs/2026-05-21-bucket-2-foundation-polish-design.md`](../specs/2026-05-21-bucket-2-foundation-polish-design.md) — Item 2.

**Prerequisites:** Cost Explorer must be enabled on the AWS account (one-time, in the Billing console). The operator's SSO profile already grants `ce:GetCostAndUsage`.

**Out of scope:** Cost budgets / alerts. Per-session attribution. JSON output. Multi-account support.

---

## File Structure

```
benchmarking/aws/runner/
├── cost.go            # NEW — CostExplorer iface, awsCostExplorer, FakeCostExplorer, Summarise, Print
├── cost_test.go       # NEW — table-driven tests with FakeCostExplorer
└── main.go            # MODIFY — replace costCheckCmd stub at L357

benchmarking/aws/Taskfile.yml   # MODIFY — add aws:cost-check task
```

## Conventions

- License header on every new `.go` file matches the existing runner files (BSL block, lines 1-4 of `stats.go`).
- Test naming: `TestX_Y` where `X` is the function under test and `Y` is the case.
- All AWS calls go through the `CostExplorer` interface; never call SDK directly from non-test code.

---

## Task 1: Define CostExplorer interface, types, and FakeCostExplorer

**Files:**
- Create: `benchmarking/aws/runner/cost.go`

- [ ] **Step 1: Create cost.go with types and interface**

```go
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
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./benchmarking/aws/runner/`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/runner/cost.go
git commit -m "feat(bench/aws): add CostExplorer interface + helpers for cost-check"
```

---

## Task 2: TDD — Summarise function

**Files:**
- Modify: `benchmarking/aws/runner/cost.go` (add Summarise + Summarise calling logic)
- Create: `benchmarking/aws/runner/cost_test.go`

- [ ] **Step 1: Write the failing test**

Create `cost_test.go` with the following content:

```go
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

	report, err := Summarise(context.Background(), fake, "us-east-2", now)
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
	report, err := Summarise(context.Background(), fake, "us-east-2", now)
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
	_, err := Summarise(context.Background(), fake, "us-east-2", now)
	require.Error(t, err)
}

var errBoom = errBoomImpl("boom")

type errBoomImpl string

func (e errBoomImpl) Error() string { return string(e) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestSummarise -v`
Expected: FAIL — `Summarise` not declared.

- [ ] **Step 3: Implement Summarise in cost.go**

Append to `cost.go`:

```go
import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
)
```

(Merge into the existing import block; `aws` and `cetypes` are new, others already imported.)

Then add the function:

```go
// Summarise issues two CE queries against the project's tag filter, then
// folds the daily totals into today / 7d / MTD buckets and surfaces the
// usage-type breakdown verbatim. `now` is injected so tests are deterministic.
func Summarise(ctx context.Context, ce CostExplorer, region string, now time.Time) (CostReport, error) {
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
		if date >= sevenDaysAgo {
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
			Start: aws.String(sevenDaysAgo),
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./benchmarking/aws/runner/ -run TestSummarise -v`
Expected: PASS for all 3 cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/cost.go benchmarking/aws/runner/cost_test.go
git commit -m "feat(bench/aws): Summarise — fold CE daily totals into today/7d/MTD"
```

---

## Task 3: TDD — Print function with golden output

**Files:**
- Modify: `benchmarking/aws/runner/cost.go`
- Modify: `benchmarking/aws/runner/cost_test.go`

- [ ] **Step 1: Write the failing test**

Append to `cost_test.go`:

```go
import (
	"bytes"
	// ... existing imports
)

func TestPrint_DeterministicLayout(t *testing.T) {
	report := CostReport{
		Region:       "us-east-2",
		Today:        0.00,
		Last7Days:    18.42,
		MonthToDate:  42.91,
		CurrencyCode: "USD",
		ByUsageType: []UsageTypeCost{
			{UsageType: "EC2 (c8g.4xlarge)", Cost: 9.20},
			{UsageType: "RDS (db.r6g.2xlarge)", Cost: 7.10},
			{UsageType: "EBS (gp3 + snapshots)", Cost: 1.45},
			{UsageType: "Data Transfer", Cost: 0.42},
			{UsageType: "Other", Cost: 0.25},
		},
	}
	var buf bytes.Buffer
	Print(&buf, report)
	want := `AWS spend — Project=redpanda-connect-bench (us-east-2)
Note: AWS Cost Explorer lags ~24-48h; today's spend will be partial.

  today         $0.00
  last 7 days   $18.42
  month-to-date $42.91

By usage type (last 7 days):
  EC2 (c8g.4xlarge)     $9.20
  RDS (db.r6g.2xlarge)  $7.10
  EBS (gp3 + snapshots) $1.45
  Data Transfer         $0.42
  Other                 $0.25
`
	require.Equal(t, want, buf.String())
}

func TestPrint_EmptyBreakdown(t *testing.T) {
	var buf bytes.Buffer
	Print(&buf, CostReport{Region: "us-east-2", CurrencyCode: "USD"})
	out := buf.String()
	require.Contains(t, out, "today         $0.00")
	require.NotContains(t, out, "By usage type") // skip section when nothing to show
}
```

(Add `"bytes"` to the existing import block, keep alphabetical order.)

- [ ] **Step 2: Run test, verify it fails**

Run: `go test ./benchmarking/aws/runner/ -run TestPrint -v`
Expected: FAIL — `Print` not declared.

- [ ] **Step 3: Implement Print in cost.go**

```go
import (
	"io"
	"sort"
	// ... existing
)

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
```

- [ ] **Step 4: Run test to verify pass**

Run: `go test ./benchmarking/aws/runner/ -run TestPrint -v`
Expected: PASS both cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/cost.go benchmarking/aws/runner/cost_test.go
git commit -m "feat(bench/aws): Print — column-aligned CostReport output"
```

---

## Task 4: awsCostExplorer impl (real SDK)

**Files:**
- Modify: `benchmarking/aws/runner/cost.go`

- [ ] **Step 1: Add awsCostExplorer and constructor**

Append to `cost.go`:

```go
import (
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

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
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./benchmarking/aws/runner/`
Expected: no output.

- [ ] **Step 3: Verify go.mod is satisfied**

Run: `go mod tidy && go build ./benchmarking/aws/runner/`
Expected: no output. The costexplorer module is part of `aws-sdk-go-v2`, already in `go.sum` via other modules. If `go mod tidy` adds a new direct dependency line, that's expected.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/cost.go go.mod go.sum
git commit -m "feat(bench/aws): wire awsCostExplorer (real SDK adapter)"
```

---

## Task 5: Replace cost-check stub in main.go

**Files:**
- Modify: `benchmarking/aws/runner/main.go` (replace `costCheckCmd` at L357)

- [ ] **Step 1: Replace the stub**

Find:

```go
func costCheckCmd(_ []string) error {
	fmt.Println("cost-check not implemented in foundation plan — see future iterations")
	return nil
}
```

Replace with:

```go
func costCheckCmd(args []string) error {
	fs := flag.NewFlagSet("cost-check", flag.ExitOnError)
	region := fs.String("region", "us-east-2", "AWS region (informational; CE is global)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	ce, err := NewAWSCostExplorer(ctx)
	if err != nil {
		return fmt.Errorf("init cost explorer: %w", err)
	}
	report, err := Summarise(ctx, ce, *region, time.Now())
	if err != nil {
		return err
	}
	Print(os.Stdout, report)
	return nil
}
```

`context`, `flag`, `time` are already imported in `main.go`; verify no missing imports.

- [ ] **Step 2: Build to verify**

Run: `go build ./benchmarking/aws/runner/`
Expected: no output.

- [ ] **Step 3: Smoke test the CLI**

Run: `go run ./benchmarking/aws/runner cost-check --help`
Expected: usage text for the `cost-check` flag set; exits 0.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): cost-check subcommand calls Summarise + Print"
```

---

## Task 6: Add aws:cost-check task

**Files:**
- Modify: `benchmarking/aws/Taskfile.yml`

- [ ] **Step 1: Add the task**

Inspect the existing `aws:validate` or `aws:down` block to match style, then append (or insert near the other `aws:*` entries) the following task:

```yaml
  cost-check:
    desc: "Show AWS Cost Explorer spend for the bench project (today/7d/MTD)"
    cmds:
      - cd ../.. && go run ./benchmarking/aws/runner cost-check
```

The task name within the `aws:` namespace yields `task aws:cost-check`.

- [ ] **Step 2: Smoke test via task runner**

Run from `benchmarking/aws/`: `task aws:cost-check --help`
Expected: the subcommand's flag-set help; exit 0.

> Note: actually invoking against AWS (e.g. `aws-vault exec ... task aws:cost-check`) is the operator's smoke test; not part of unit-test automation.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/Taskfile.yml
git commit -m "feat(bench/aws): expose cost-check via task aws:cost-check"
```

---

## Task 7: Document cost-check in README

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Add a section**

Find the existing "Operator commands" / "Tasks" section (whichever exists), and add a row or paragraph documenting cost-check. If the existing table is:

```markdown
| Task | Purpose |
|---|---|
| `task aws:bench scenario=…` | Run a single scenario end-to-end |
| `task aws:validate scenario=…` | Lint a scenario YAML (no AWS calls) |
| `task aws:down scenario=…` | Tear down infra for the scenario's stack |
| `task aws:cost-check` | (not implemented in foundation) |
```

Replace the last row with:

```markdown
| `task aws:cost-check` | Print AWS Cost Explorer spend tagged `Project=redpanda-connect-bench` (today / last 7d / month-to-date + usage-type breakdown). CE lags 24-48h. |
```

If no such table exists, add a short subsection under "Operator commands":

```markdown
### Cost check

```sh
aws-vault exec AWSAdministratorAccess-605419575229 -- task aws:cost-check
```

Prints today / last-7-days / month-to-date spend filtered on `Project=redpanda-connect-bench` plus a per-usage-type breakdown. Cost Explorer lags ~24-48h, so a bench you just ran won't show until tomorrow.
```

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): document task aws:cost-check"
```

---

## Verification checklist (end of plan)

- [ ] `go test ./benchmarking/aws/runner/ -run 'TestSummarise|TestPrint' -v` — all pass
- [ ] `go vet ./benchmarking/aws/runner/` — clean
- [ ] `go build ./benchmarking/aws/runner/` — clean
- [ ] `gofmt -l benchmarking/aws/runner/cost.go benchmarking/aws/runner/cost_test.go` — empty
- [ ] `task aws:cost-check --help` — shows flag-set help
- [ ] (Operator) `aws-vault exec … task aws:cost-check` — real output, no errors
- [ ] README section is present and accurate

## Done criteria

`task aws:cost-check` prints a real Cost Explorer summary with today/7d/MTD totals and a per-usage-type breakdown, with no live-AWS code paths in the unit tests.
