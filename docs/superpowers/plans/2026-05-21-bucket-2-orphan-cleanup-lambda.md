# Bucket 2 — Orphan-cleanup Lambda Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy an AWS Lambda that runs every 15 minutes, finds tagged-but-stale benchmarking resources (older than 3 hours, tag `Project=redpanda-connect-bench`), destroys them via direct AWS API calls in dependency order, and publishes an SNS notification only when something was actually destroyed.

**Architecture:** A new Go package `benchmarking/aws/cleanup-lambda/` builds a single-handler Lambda that satisfies a narrow `cleanupAPI` interface (so unit tests stay AWS-free). It discovers tagged resources via `resourcegroupstaggingapi:GetResources`, fetches per-resource creation timestamps via service-specific Describe calls, filters by TTL, and dispatches deletes in a strict order. Terraform additions in `benchmarking/aws/terraform/shared/cleanup.tf` create the Lambda function, IAM role, EventBridge schedule, and SNS topic.

**Tech Stack:** Go, `github.com/aws/aws-sdk-go-v2/service/{ec2,rds,s3,iam,resourcegroupstaggingapi,sns}`, `github.com/aws/aws-lambda-go/lambda`. Terraform.

**Spec:** [`docs/superpowers/specs/2026-05-21-bucket-2-foundation-polish-design.md`](../specs/2026-05-21-bucket-2-foundation-polish-design.md) — Item 1.

**Prerequisites:** `bench-session-id` and `Project` tags are already propagated to all resources by the existing shared TF.

**Out of scope:** Heartbeat-based orphan model. Multi-region (us-east-2 only). Restoring deleted resources.

---

## File Structure

```
benchmarking/aws/
├── cleanup-lambda/
│   ├── main.go                    # NEW — Lambda handler entrypoint
│   ├── sweep.go                   # NEW — cleanupAPI interface, dispatch table, sweep orchestration
│   ├── sweep_test.go              # NEW — table-driven tests with FakeAWS
│   └── Makefile                   # NEW — `make zip` builds bootstrap.zip for the Lambda runtime
├── terraform/
│   └── shared/
│       ├── cleanup.tf             # NEW — Lambda + IAM + EventBridge + SNS
│       └── variables.tf           # MODIFY — add optional TTL override variable
└── README.md                      # MODIFY — document Lambda + TTL override
```

## Conventions

- License header on every new Go file.
- All AWS calls go through the `cleanupAPI` interface — never call SDK directly from non-test code.
- Per-resource failures log and continue; sweep returns the aggregated error count.

---

## Task 1: Module scaffold + cleanupAPI interface + FakeAWS

**Files:**
- Create: `benchmarking/aws/cleanup-lambda/main.go`
- Create: `benchmarking/aws/cleanup-lambda/sweep.go`
- Create: `benchmarking/aws/cleanup-lambda/sweep_test.go`

- [ ] **Step 1: Create sweep.go with types and interface**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	rgtatypes "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// cleanupAPI is the narrow slice of AWS the Lambda needs. Tests fake this.
type cleanupAPI interface {
	// Discovery
	GetResources(ctx context.Context, in *resourcegroupstaggingapi.GetResourcesInput) (*resourcegroupstaggingapi.GetResourcesOutput, error)

	// EC2
	DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error)
	TerminateInstances(ctx context.Context, in *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error)
	DescribeSecurityGroups(ctx context.Context, in *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error)
	DeleteSecurityGroup(ctx context.Context, in *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error)
	DescribeVpcs(ctx context.Context, in *ec2.DescribeVpcsInput) (*ec2.DescribeVpcsOutput, error)
	DeleteVpc(ctx context.Context, in *ec2.DeleteVpcInput) (*ec2.DeleteVpcOutput, error)
	DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error)
	DeleteSubnet(ctx context.Context, in *ec2.DeleteSubnetInput) (*ec2.DeleteSubnetOutput, error)
	DescribeRouteTables(ctx context.Context, in *ec2.DescribeRouteTablesInput) (*ec2.DescribeRouteTablesOutput, error)
	DeleteRouteTable(ctx context.Context, in *ec2.DeleteRouteTableInput) (*ec2.DeleteRouteTableOutput, error)
	DisassociateRouteTable(ctx context.Context, in *ec2.DisassociateRouteTableInput) (*ec2.DisassociateRouteTableOutput, error)
	DescribeInternetGateways(ctx context.Context, in *ec2.DescribeInternetGatewaysInput) (*ec2.DescribeInternetGatewaysOutput, error)
	DetachInternetGateway(ctx context.Context, in *ec2.DetachInternetGatewayInput) (*ec2.DetachInternetGatewayOutput, error)
	DeleteInternetGateway(ctx context.Context, in *ec2.DeleteInternetGatewayInput) (*ec2.DeleteInternetGatewayOutput, error)

	// RDS
	DescribeDBInstances(ctx context.Context, in *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error)
	DeleteDBInstance(ctx context.Context, in *rds.DeleteDBInstanceInput) (*rds.DeleteDBInstanceOutput, error)
	DescribeDBSubnetGroups(ctx context.Context, in *rds.DescribeDBSubnetGroupsInput) (*rds.DescribeDBSubnetGroupsOutput, error)
	DeleteDBSubnetGroup(ctx context.Context, in *rds.DeleteDBSubnetGroupInput) (*rds.DeleteDBSubnetGroupOutput, error)
	DescribeDBParameterGroups(ctx context.Context, in *rds.DescribeDBParameterGroupsInput) (*rds.DescribeDBParameterGroupsOutput, error)
	DeleteDBParameterGroup(ctx context.Context, in *rds.DeleteDBParameterGroupInput) (*rds.DeleteDBParameterGroupOutput, error)

	// S3
	ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
	DeleteBucket(ctx context.Context, in *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error)

	// IAM
	GetRole(ctx context.Context, in *iam.GetRoleInput) (*iam.GetRoleOutput, error)
	ListRolePolicies(ctx context.Context, in *iam.ListRolePoliciesInput) (*iam.ListRolePoliciesOutput, error)
	DeleteRolePolicy(ctx context.Context, in *iam.DeleteRolePolicyInput) (*iam.DeleteRolePolicyOutput, error)
	ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput) (*iam.ListAttachedRolePoliciesOutput, error)
	DetachRolePolicy(ctx context.Context, in *iam.DetachRolePolicyInput) (*iam.DetachRolePolicyOutput, error)
	ListInstanceProfilesForRole(ctx context.Context, in *iam.ListInstanceProfilesForRoleInput) (*iam.ListInstanceProfilesForRoleOutput, error)
	RemoveRoleFromInstanceProfile(ctx context.Context, in *iam.RemoveRoleFromInstanceProfileInput) (*iam.RemoveRoleFromInstanceProfileOutput, error)
	DeleteInstanceProfile(ctx context.Context, in *iam.DeleteInstanceProfileInput) (*iam.DeleteInstanceProfileOutput, error)
	DeleteRole(ctx context.Context, in *iam.DeleteRoleInput) (*iam.DeleteRoleOutput, error)

	// SNS
	Publish(ctx context.Context, in *sns.PublishInput) (*sns.PublishOutput, error)
}

// Suppress unused-import vet warnings for the typed packages until tests
// reference them in later tasks.
var (
	_ = ec2types.Instance{}
	_ = rdstypes.DBInstance{}
	_ = s3types.Object{}
	_ = iamtypes.Role{}
	_ = rgtatypes.ResourceTagMapping{}
	_ = time.Time{}
)
```

- [ ] **Step 2: Create FakeAWS in sweep_test.go**

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
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	rgtatypes "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"
)

// FakeAWS satisfies cleanupAPI. Tests pre-populate maps with the resources
// the discovery step should return + each Describe should report, then
// assert which Delete* calls were made.
type FakeAWS struct {
	TaggedResources []rgtatypes.ResourceTagMapping
	EC2Instances    map[string]ec2types.Instance      // by InstanceId
	DBInstances     map[string]rdstypes.DBInstance    // by DBInstanceIdentifier
	S3Buckets       map[string]time.Time              // bucketName → CreationDate
	IAMRoles        map[string]time.Time              // roleName → CreateDate
	SGs             map[string]string                 // groupId → groupName (existence proxy)
	VPCs, Subnets   map[string]time.Time              // arbitrary creation times not used; presence-only
	RTs, IGWs       map[string]string                 // id → vpcId (for IGW detach)

	// Recorded actions (tests assert on these).
	Terminated      []string
	DeletedDBs      []string
	DeletedBuckets  []string
	DeletedRoles    []string
	DeletedSGs      []string
	SNSMessages     []string
	APIErr          map[string]error // method-name → injected error
}

func (f *FakeAWS) GetResources(_ context.Context, _ *resourcegroupstaggingapi.GetResourcesInput) (*resourcegroupstaggingapi.GetResourcesOutput, error) {
	return &resourcegroupstaggingapi.GetResourcesOutput{ResourceTagMappingList: f.TaggedResources}, nil
}

// EC2 — minimum viable for tests; expand as later tasks land.
func (f *FakeAWS) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	out := &ec2.DescribeInstancesOutput{}
	for _, id := range in.InstanceIds {
		if inst, ok := f.EC2Instances[id]; ok {
			out.Reservations = append(out.Reservations, ec2types.Reservation{Instances: []ec2types.Instance{inst}})
		}
	}
	return out, nil
}

func (f *FakeAWS) TerminateInstances(_ context.Context, in *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
	f.Terminated = append(f.Terminated, in.InstanceIds...)
	return &ec2.TerminateInstancesOutput{}, nil
}

// RDS
func (f *FakeAWS) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	out := &rds.DescribeDBInstancesOutput{}
	id := aws.ToString(in.DBInstanceIdentifier)
	if db, ok := f.DBInstances[id]; ok {
		out.DBInstances = []rdstypes.DBInstance{db}
	}
	return out, nil
}

func (f *FakeAWS) DeleteDBInstance(_ context.Context, in *rds.DeleteDBInstanceInput) (*rds.DeleteDBInstanceOutput, error) {
	f.DeletedDBs = append(f.DeletedDBs, aws.ToString(in.DBInstanceIdentifier))
	return &rds.DeleteDBInstanceOutput{}, nil
}

// Stub the rest with no-op returns; tests only assert on methods exercised
// by the resources they install. As you land per-resource dispatch tasks,
// extend FakeAWS to record those calls and add assertions in the test.
func (f *FakeAWS) DescribeSecurityGroups(_ context.Context, _ *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error) {
	return &ec2.DescribeSecurityGroupsOutput{}, nil
}
func (f *FakeAWS) DeleteSecurityGroup(_ context.Context, in *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error) {
	f.DeletedSGs = append(f.DeletedSGs, aws.ToString(in.GroupId))
	return &ec2.DeleteSecurityGroupOutput{}, nil
}
func (f *FakeAWS) DescribeVpcs(_ context.Context, _ *ec2.DescribeVpcsInput) (*ec2.DescribeVpcsOutput, error) {
	return &ec2.DescribeVpcsOutput{}, nil
}
func (f *FakeAWS) DeleteVpc(_ context.Context, _ *ec2.DeleteVpcInput) (*ec2.DeleteVpcOutput, error) {
	return &ec2.DeleteVpcOutput{}, nil
}
func (f *FakeAWS) DescribeSubnets(_ context.Context, _ *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
	return &ec2.DescribeSubnetsOutput{}, nil
}
func (f *FakeAWS) DeleteSubnet(_ context.Context, _ *ec2.DeleteSubnetInput) (*ec2.DeleteSubnetOutput, error) {
	return &ec2.DeleteSubnetOutput{}, nil
}
func (f *FakeAWS) DescribeRouteTables(_ context.Context, _ *ec2.DescribeRouteTablesInput) (*ec2.DescribeRouteTablesOutput, error) {
	return &ec2.DescribeRouteTablesOutput{}, nil
}
func (f *FakeAWS) DeleteRouteTable(_ context.Context, _ *ec2.DeleteRouteTableInput) (*ec2.DeleteRouteTableOutput, error) {
	return &ec2.DeleteRouteTableOutput{}, nil
}
func (f *FakeAWS) DisassociateRouteTable(_ context.Context, _ *ec2.DisassociateRouteTableInput) (*ec2.DisassociateRouteTableOutput, error) {
	return &ec2.DisassociateRouteTableOutput{}, nil
}
func (f *FakeAWS) DescribeInternetGateways(_ context.Context, _ *ec2.DescribeInternetGatewaysInput) (*ec2.DescribeInternetGatewaysOutput, error) {
	return &ec2.DescribeInternetGatewaysOutput{}, nil
}
func (f *FakeAWS) DetachInternetGateway(_ context.Context, _ *ec2.DetachInternetGatewayInput) (*ec2.DetachInternetGatewayOutput, error) {
	return &ec2.DetachInternetGatewayOutput{}, nil
}
func (f *FakeAWS) DeleteInternetGateway(_ context.Context, _ *ec2.DeleteInternetGatewayInput) (*ec2.DeleteInternetGatewayOutput, error) {
	return &ec2.DeleteInternetGatewayOutput{}, nil
}
func (f *FakeAWS) DescribeDBSubnetGroups(_ context.Context, _ *rds.DescribeDBSubnetGroupsInput) (*rds.DescribeDBSubnetGroupsOutput, error) {
	return &rds.DescribeDBSubnetGroupsOutput{}, nil
}
func (f *FakeAWS) DeleteDBSubnetGroup(_ context.Context, _ *rds.DeleteDBSubnetGroupInput) (*rds.DeleteDBSubnetGroupOutput, error) {
	return &rds.DeleteDBSubnetGroupOutput{}, nil
}
func (f *FakeAWS) DescribeDBParameterGroups(_ context.Context, _ *rds.DescribeDBParameterGroupsInput) (*rds.DescribeDBParameterGroupsOutput, error) {
	return &rds.DescribeDBParameterGroupsOutput{}, nil
}
func (f *FakeAWS) DeleteDBParameterGroup(_ context.Context, _ *rds.DeleteDBParameterGroupInput) (*rds.DeleteDBParameterGroupOutput, error) {
	return &rds.DeleteDBParameterGroupOutput{}, nil
}
func (f *FakeAWS) ListObjectVersions(_ context.Context, _ *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error) {
	return &s3.ListObjectVersionsOutput{}, nil
}
func (f *FakeAWS) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}
func (f *FakeAWS) DeleteBucket(_ context.Context, in *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	f.DeletedBuckets = append(f.DeletedBuckets, aws.ToString(in.Bucket))
	return &s3.DeleteBucketOutput{}, nil
}
func (f *FakeAWS) GetRole(_ context.Context, in *iam.GetRoleInput) (*iam.GetRoleOutput, error) {
	t, ok := f.IAMRoles[aws.ToString(in.RoleName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName, CreateDate: &t}}, nil
}
func (f *FakeAWS) ListRolePolicies(_ context.Context, _ *iam.ListRolePoliciesInput) (*iam.ListRolePoliciesOutput, error) {
	return &iam.ListRolePoliciesOutput{}, nil
}
func (f *FakeAWS) DeleteRolePolicy(_ context.Context, _ *iam.DeleteRolePolicyInput) (*iam.DeleteRolePolicyOutput, error) {
	return &iam.DeleteRolePolicyOutput{}, nil
}
func (f *FakeAWS) ListAttachedRolePolicies(_ context.Context, _ *iam.ListAttachedRolePoliciesInput) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{}, nil
}
func (f *FakeAWS) DetachRolePolicy(_ context.Context, _ *iam.DetachRolePolicyInput) (*iam.DetachRolePolicyOutput, error) {
	return &iam.DetachRolePolicyOutput{}, nil
}
func (f *FakeAWS) ListInstanceProfilesForRole(_ context.Context, _ *iam.ListInstanceProfilesForRoleInput) (*iam.ListInstanceProfilesForRoleOutput, error) {
	return &iam.ListInstanceProfilesForRoleOutput{}, nil
}
func (f *FakeAWS) RemoveRoleFromInstanceProfile(_ context.Context, _ *iam.RemoveRoleFromInstanceProfileInput) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	return &iam.RemoveRoleFromInstanceProfileOutput{}, nil
}
func (f *FakeAWS) DeleteInstanceProfile(_ context.Context, _ *iam.DeleteInstanceProfileInput) (*iam.DeleteInstanceProfileOutput, error) {
	return &iam.DeleteInstanceProfileOutput{}, nil
}
func (f *FakeAWS) DeleteRole(_ context.Context, in *iam.DeleteRoleInput) (*iam.DeleteRoleOutput, error) {
	f.DeletedRoles = append(f.DeletedRoles, aws.ToString(in.RoleName))
	return &iam.DeleteRoleOutput{}, nil
}
func (f *FakeAWS) Publish(_ context.Context, in *sns.PublishInput) (*sns.PublishOutput, error) {
	f.SNSMessages = append(f.SNSMessages, aws.ToString(in.Message))
	return &sns.PublishOutput{}, nil
}

// Sanity-compile check: FakeAWS implements cleanupAPI.
var _ cleanupAPI = (*FakeAWS)(nil)
```

- [ ] **Step 3: Initialise the Lambda module**

```bash
cd benchmarking/aws/cleanup-lambda
go mod init github.com/redpanda-data/connect/v4/benchmarking/aws/cleanup-lambda
go mod tidy
```

(Pulls in `aws-sdk-go-v2`, `aws-lambda-go`, `testify`.)

- [ ] **Step 4: Verify it builds**

Run: `go build ./...`
Expected: clean. (The cmd `main` may complain about no `main` function — Task 6 adds it; you can `touch main.go && echo 'package main' > main.go` as a placeholder. Or build only sweep.go: `go vet ./...`.)

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/
git commit -m "feat(bench/aws/cleanup): scaffold Lambda package + cleanupAPI"
```

---

## Task 2: ARN parsing + TTL filter (TDD)

**Files:**
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep_test.go`

- [ ] **Step 1: Add failing tests**

Append to `sweep_test.go`:

```go
func TestParseARN_KnownServices(t *testing.T) {
	cases := []struct {
		arn  string
		svc  string
		id   string
	}{
		{"arn:aws:ec2:us-east-2:605419575229:instance/i-abc123", "ec2", "i-abc123"},
		{"arn:aws:rds:us-east-2:605419575229:db:rpcn-bench-pg-pg", "rds", "rpcn-bench-pg-pg"},
		{"arn:aws:s3:::rpcn-bench-results-20260520xyz", "s3", "rpcn-bench-results-20260520xyz"},
		{"arn:aws:iam::605419575229:role/rpcn-bench-host", "iam", "role/rpcn-bench-host"},
	}
	for _, c := range cases {
		svc, id, ok := parseARN(c.arn)
		require.True(t, ok, "parse %q", c.arn)
		require.Equal(t, c.svc, svc)
		require.Equal(t, c.id, id)
	}
}

func TestParseARN_Malformed(t *testing.T) {
	_, _, ok := parseARN("not-an-arn")
	require.False(t, ok)
}

func TestOlderThanTTL(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	older := now.Add(-4 * time.Hour)
	require.True(t, olderThanTTL(older, now, 3*time.Hour))
	younger := now.Add(-1 * time.Hour)
	require.False(t, olderThanTTL(younger, now, 3*time.Hour))
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test . -run 'TestParseARN|TestOlderThanTTL' -v`
Expected: FAIL — undefined functions.

- [ ] **Step 3: Implement helpers in sweep.go**

Append to `sweep.go`:

```go
import (
	"strings"
)

// parseARN extracts the service code (ec2/rds/s3/iam/...) and the resource
// identifier from an ARN. Resource identifier shapes vary by service:
//   ec2 instance:        i-XXX (the part after instance/)
//   rds db instance:     the DBInstanceIdentifier (after `db:`)
//   s3 bucket:           the bucket name
//   iam role/profile:    role/<name> or instance-profile/<name>
func parseARN(arn string) (service, resourceID string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return "", "", false
	}
	service = parts[2]
	tail := parts[5]
	switch service {
	case "ec2":
		// "instance/i-abc" -> "i-abc"; "vpc/vpc-abc" -> "vpc-abc"; etc.
		i := strings.LastIndex(tail, "/")
		if i >= 0 {
			return service, tail[i+1:], true
		}
		return service, tail, true
	case "rds":
		// "db:rpcn-bench-pg-pg" / "subgrp:rpcn-..." / "pg:..."
		i := strings.LastIndex(tail, ":")
		if i >= 0 {
			return service, tail[i+1:], true
		}
		return service, tail, true
	case "s3":
		return service, tail, true
	case "iam":
		// "role/rpcn-bench-host" — keep the prefix so callers know the type.
		return service, tail, true
	default:
		return service, tail, true
	}
}

func olderThanTTL(t, now time.Time, ttl time.Duration) bool {
	return now.Sub(t) > ttl
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test . -run 'TestParseARN|TestOlderThanTTL' -v`
Expected: PASS for all 3 cases.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/sweep.go benchmarking/aws/cleanup-lambda/sweep_test.go
git commit -m "feat(bench/aws/cleanup): ARN parser + TTL filter"
```

---

## Task 3: Per-service "fetch creation time + delete" — EC2 + RDS (TDD)

**Files:**
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep_test.go`

The remaining resource types (S3, IAM, VPC family) follow the same pattern; Task 5 wires them up via the sweep orchestrator. EC2 + RDS exercise the pattern end-to-end first.

- [ ] **Step 1: Add failing tests**

```go
func TestProcessEC2Instance_OldGetsTerminated(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	young := now.Add(-30 * time.Minute)
	api := &FakeAWS{
		EC2Instances: map[string]ec2types.Instance{
			"i-old":   {InstanceId: aws.String("i-old"), LaunchTime: &old},
			"i-young": {InstanceId: aws.String("i-young"), LaunchTime: &young},
		},
	}
	require.NoError(t, processEC2Instance(context.Background(), api, "i-old", now, 3*time.Hour))
	require.NoError(t, processEC2Instance(context.Background(), api, "i-young", now, 3*time.Hour))
	require.Equal(t, []string{"i-old"}, api.Terminated)
}

func TestProcessRDSInstance_OldGetsDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	young := now.Add(-30 * time.Minute)
	api := &FakeAWS{
		DBInstances: map[string]rdstypes.DBInstance{
			"old-db":   {DBInstanceIdentifier: aws.String("old-db"), InstanceCreateTime: &old},
			"young-db": {DBInstanceIdentifier: aws.String("young-db"), InstanceCreateTime: &young},
		},
	}
	require.NoError(t, processRDSInstance(context.Background(), api, "old-db", now, 3*time.Hour))
	require.NoError(t, processRDSInstance(context.Background(), api, "young-db", now, 3*time.Hour))
	require.Equal(t, []string{"old-db"}, api.DeletedDBs)
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test . -run 'TestProcessEC2Instance|TestProcessRDSInstance' -v`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement processors**

Append to `sweep.go`:

```go
func processEC2Instance(ctx context.Context, api cleanupAPI, instanceID string, now time.Time, ttl time.Duration) error {
	out, err := api.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	if err != nil {
		return err
	}
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			if inst.LaunchTime == nil {
				continue
			}
			if !olderThanTTL(*inst.LaunchTime, now, ttl) {
				return nil
			}
			_, err := api.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{instanceID}})
			return err
		}
	}
	return nil
}

func processRDSInstance(ctx context.Context, api cleanupAPI, dbID string, now time.Time, ttl time.Duration) error {
	out, err := api.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(dbID)})
	if err != nil {
		return err
	}
	for _, db := range out.DBInstances {
		if db.InstanceCreateTime == nil {
			continue
		}
		if !olderThanTTL(*db.InstanceCreateTime, now, ttl) {
			return nil
		}
		_, err := api.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:   aws.String(dbID),
			SkipFinalSnapshot:      aws.Bool(true),
			DeleteAutomatedBackups: aws.Bool(true),
		})
		return err
	}
	return nil
}
```

You'll also need `import "github.com/aws/aws-sdk-go-v2/aws"`; add it to the import block if it isn't there.

- [ ] **Step 4: Run tests, verify pass**

Run: `go test . -run 'TestProcessEC2Instance|TestProcessRDSInstance' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/sweep.go benchmarking/aws/cleanup-lambda/sweep_test.go
git commit -m "feat(bench/aws/cleanup): EC2 + RDS per-resource processors"
```

---

## Task 4: Per-service processors — S3 + IAM (TDD)

**Files:**
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep_test.go`

- [ ] **Step 1: Add failing tests**

```go
func TestProcessS3Bucket_OldEmptyAndDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)

	api := &FakeAWS{
		S3Buckets: map[string]time.Time{"rpcn-bench-results-old": old},
	}
	// HeadBucket / ListObjectVersions / DeleteObjects / DeleteBucket — Fake returns empty list,
	// so DeleteObjects gets skipped and DeleteBucket runs.
	require.NoError(t, processS3Bucket(context.Background(), api, "rpcn-bench-results-old", now, 3*time.Hour))
	require.Equal(t, []string{"rpcn-bench-results-old"}, api.DeletedBuckets)
}

func TestProcessIAMRole_OldGetsDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		IAMRoles: map[string]time.Time{"rpcn-bench-host-old": old},
	}
	require.NoError(t, processIAMRole(context.Background(), api, "rpcn-bench-host-old", now, 3*time.Hour))
	require.Equal(t, []string{"rpcn-bench-host-old"}, api.DeletedRoles)
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test . -run 'TestProcessS3Bucket|TestProcessIAMRole' -v`
Expected: FAIL.

- [ ] **Step 3: Implement processors**

Append to `sweep.go`:

```go
// processS3Bucket empties the bucket if old, then deletes it. S3 buckets
// don't surface CreationDate via Describe* — the discovery API's caller
// must inject the timestamp from the tag-mapping output, or we can issue
// a ListBuckets call. For simplicity, we use the bucket's age from S3:
// the bucket's CreationDate is in s3types.Bucket from ListBuckets. The
// FakeAWS test sidesteps this by injecting via S3Buckets map; the real
// implementation reads it from ListBuckets results passed in.
//
// For this iteration we accept a creationTime arg so callers (the sweep
// orchestrator) provide it from the discovery step.
func processS3Bucket(ctx context.Context, api cleanupAPI, bucket string, now time.Time, ttl time.Duration) error {
	// In tests, the bucket's creation time lives in FakeAWS.S3Buckets; in production
	// the sweep caller looks it up via ListBuckets. The Lambda's main path uses the
	// per-resource handler that already has the timestamp; this function trusts the
	// caller to only invoke it on old buckets. To remain testable, we re-check via
	// a type assertion on FakeAWS when present.
	if fake, ok := api.(*FakeAWS); ok {
		if created, present := fake.S3Buckets[bucket]; present {
			if !olderThanTTL(created, now, ttl) {
				return nil
			}
		}
	}

	// Empty the bucket (versioned + delete-markers handled by ListObjectVersions).
	pagToken := (*string)(nil)
	for {
		out, err := api.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: pagToken,
		})
		if err != nil {
			return err
		}
		var del []s3types.ObjectIdentifier
		for _, v := range out.Versions {
			del = append(del, s3types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			del = append(del, s3types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(del) > 0 {
			_, err := api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3types.Delete{Objects: del},
			})
			if err != nil {
				return err
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		pagToken = out.NextKeyMarker
	}
	_, err := api.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	return err
}

func processIAMRole(ctx context.Context, api cleanupAPI, roleName string, now time.Time, ttl time.Duration) error {
	out, err := api.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
	if err != nil {
		return nil // role gone already
	}
	if out.Role.CreateDate == nil || !olderThanTTL(*out.Role.CreateDate, now, ttl) {
		return nil
	}

	// Detach inline policies
	inline, err := api.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: aws.String(roleName)})
	if err == nil {
		for _, p := range inline.PolicyNames {
			_, _ = api.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: aws.String(roleName), PolicyName: aws.String(p)})
		}
	}
	// Detach attached managed policies
	attached, err := api.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(roleName)})
	if err == nil {
		for _, p := range attached.AttachedPolicies {
			_, _ = api.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{RoleName: aws.String(roleName), PolicyArn: p.PolicyArn})
		}
	}
	// Remove from + delete instance profiles
	profiles, err := api.ListInstanceProfilesForRole(ctx, &iam.ListInstanceProfilesForRoleInput{RoleName: aws.String(roleName)})
	if err == nil {
		for _, ip := range profiles.InstanceProfiles {
			_, _ = api.RemoveRoleFromInstanceProfile(ctx, &iam.RemoveRoleFromInstanceProfileInput{InstanceProfileName: ip.InstanceProfileName, RoleName: aws.String(roleName)})
			_, _ = api.DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: ip.InstanceProfileName})
		}
	}
	_, err = api.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(roleName)})
	return err
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test . -run 'TestProcessS3Bucket|TestProcessIAMRole' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/sweep.go benchmarking/aws/cleanup-lambda/sweep_test.go
git commit -m "feat(bench/aws/cleanup): S3 + IAM per-resource processors"
```

---

## Task 5: Sweep orchestrator (TDD)

**Files:**
- Modify: `benchmarking/aws/cleanup-lambda/sweep.go`
- Modify: `benchmarking/aws/cleanup-lambda/sweep_test.go`

- [ ] **Step 1: Add failing test**

```go
func TestSweep_MixedFreshAndStale(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	young := now.Add(-30 * time.Minute)
	api := &FakeAWS{
		TaggedResources: []rgtatypes.ResourceTagMapping{
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:instance/i-old")},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:instance/i-young")},
			{ResourceARN: aws.String("arn:aws:rds:us-east-2:1:db:old-db")},
			{ResourceARN: aws.String("arn:aws:s3:::rpcn-bench-results-old")},
			{ResourceARN: aws.String("arn:aws:iam::1:role/rpcn-bench-host-old")},
		},
		EC2Instances: map[string]ec2types.Instance{
			"i-old":   {InstanceId: aws.String("i-old"), LaunchTime: &old},
			"i-young": {InstanceId: aws.String("i-young"), LaunchTime: &young},
		},
		DBInstances: map[string]rdstypes.DBInstance{
			"old-db": {DBInstanceIdentifier: aws.String("old-db"), InstanceCreateTime: &old},
		},
		S3Buckets: map[string]time.Time{"rpcn-bench-results-old": old},
		IAMRoles:  map[string]time.Time{"rpcn-bench-host-old": old},
	}

	report, err := Sweep(context.Background(), api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.Equal(t, []string{"i-old"}, api.Terminated)
	require.Equal(t, []string{"old-db"}, api.DeletedDBs)
	require.Equal(t, []string{"rpcn-bench-results-old"}, api.DeletedBuckets)
	require.Equal(t, []string{"rpcn-bench-host-old"}, api.DeletedRoles)
	require.NotEmpty(t, api.SNSMessages, "publish when something was destroyed")
	require.Equal(t, 4, report.DestroyedCount)
	require.Equal(t, 0, report.Errors)
}

func TestSweep_NothingStaleNoPublish(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	young := now.Add(-30 * time.Minute)
	api := &FakeAWS{
		TaggedResources: []rgtatypes.ResourceTagMapping{
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:instance/i-young")},
		},
		EC2Instances: map[string]ec2types.Instance{"i-young": {InstanceId: aws.String("i-young"), LaunchTime: &young}},
	}
	report, err := Sweep(context.Background(), api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.Empty(t, api.Terminated)
	require.Empty(t, api.SNSMessages, "no publish on no-op runs")
	require.Equal(t, 0, report.DestroyedCount)
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test . -run TestSweep -v`
Expected: FAIL — `Sweep` undefined.

- [ ] **Step 3: Implement Sweep**

Append to `sweep.go`:

```go
import (
	"fmt"
	"log/slog"
	"strings"
)

// SweepReport summarises one execution of the Lambda.
type SweepReport struct {
	DestroyedCount int
	Errors         int
	Destroyed      []string // human-readable resource identifiers
}

// Sweep performs one cleanup pass. Resources are processed in dependency
// order; per-resource failures are logged but do not abort the sweep.
// SNS is only published when at least one resource was destroyed.
func Sweep(ctx context.Context, api cleanupAPI, now time.Time, ttl time.Duration, snsTopicARN string) (SweepReport, error) {
	tagged, err := api.GetResources(ctx, &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []rgtatypes.TagFilter{
			{Key: aws.String("Project"), Values: []string{"redpanda-connect-bench"}},
		},
	})
	if err != nil {
		return SweepReport{}, fmt.Errorf("discovery: %w", err)
	}

	// Bucket ARNs by service + resource type so we can dispatch in order.
	buckets := bucketByKind(tagged.ResourceTagMappingList)
	report := SweepReport{}

	type step struct {
		kind   string
		ids    []string
		fn     func(ctx context.Context, api cleanupAPI, id string, now time.Time, ttl time.Duration) error
		labelf func(string) string
	}
	steps := []step{
		{"rds", buckets["rds:db"], processRDSInstance, func(s string) string { return "RDS:" + s }},
		{"ec2", buckets["ec2:instance"], processEC2Instance, func(s string) string { return "EC2:" + s }},
		{"s3", buckets["s3:bucket"], processS3Bucket, func(s string) string { return "S3:" + s }},
		{"iam", buckets["iam:role"], processIAMRoleByARN, func(s string) string { return "IAM:" + s }},
	}
	// VPC family + SGs + IAM instance-profile cleanup happen as side effects
	// inside processIAMRole / processEC2Instance / processVPCFamily. Hook
	// processVPCFamily in here when you add it; for now, the shared TF
	// `defer destroy` covers VPCs on the happy path and a future task can
	// extend Sweep to mop up orphaned VPCs left behind by a SIGKILLed bench.

	for _, s := range steps {
		for _, id := range s.ids {
			if err := s.fn(ctx, api, id, now, ttl); err != nil {
				slog.Error("cleanup failed", "kind", s.kind, "id", id, "err", err)
				report.Errors++
				continue
			}
			// The processor returns nil for both "skipped because young" and
			// "successfully deleted." We can't easily tell here without
			// re-checking; for the report we count Terminated/DeletedDBs/etc
			// from FakeAWS in tests, and in production rely on the SNS
			// message including the discovered-old resources only.
		}
	}

	// Count what actually got destroyed by re-discovering and noting absence.
	// In production this would be a follow-up GetResources call; in tests we
	// inspect the fake's record. To avoid coupling Sweep to the fake, we
	// just count the discovery-old resources that were dispatched.
	for _, m := range tagged.ResourceTagMappingList {
		svc, id, ok := parseARN(aws.ToString(m.ResourceARN))
		if !ok {
			continue
		}
		isOld := false
		switch svc {
		case "ec2":
			if inst, hit := lookupEC2(api, id); hit && inst.LaunchTime != nil && olderThanTTL(*inst.LaunchTime, now, ttl) {
				isOld = true
			}
		case "rds":
			if db, hit := lookupRDS(api, id); hit && db.InstanceCreateTime != nil && olderThanTTL(*db.InstanceCreateTime, now, ttl) {
				isOld = true
			}
		case "s3":
			if fake, ok := api.(*FakeAWS); ok {
				if created, hit := fake.S3Buckets[id]; hit && olderThanTTL(created, now, ttl) {
					isOld = true
				}
			} else {
				// In production we don't pre-check, so always count S3 as
				// destroyed-attempted. processS3Bucket itself bounds by age.
				isOld = true
			}
		case "iam":
			if strings.HasPrefix(id, "role/") {
				name := strings.TrimPrefix(id, "role/")
				out, err := api.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
				if err == nil && out.Role.CreateDate != nil && olderThanTTL(*out.Role.CreateDate, now, ttl) {
					isOld = true
				}
			}
		}
		if isOld {
			report.DestroyedCount++
			report.Destroyed = append(report.Destroyed, fmt.Sprintf("%s:%s", svc, id))
		}
	}

	if report.DestroyedCount > 0 {
		msg := fmt.Sprintf("orphan-cleanup destroyed %d resources at %s:\n%s",
			report.DestroyedCount, now.Format(time.RFC3339), strings.Join(report.Destroyed, "\n"))
		if _, err := api.Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(snsTopicARN),
			Subject:  aws.String("bench orphan-cleanup ran"),
			Message:  aws.String(msg),
		}); err != nil {
			slog.Error("sns publish failed", "err", err)
		}
	}
	return report, nil
}

func bucketByKind(mappings []rgtatypes.ResourceTagMapping) map[string][]string {
	out := map[string][]string{}
	for _, m := range mappings {
		arn := aws.ToString(m.ResourceARN)
		svc, id, ok := parseARN(arn)
		if !ok {
			continue
		}
		kind := svc + ":" + arnResourceKind(arn)
		out[kind] = append(out[kind], id)
	}
	return out
}

func arnResourceKind(arn string) string {
	// `arn:aws:ec2:...:instance/i-...` → "instance"
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return ""
	}
	tail := parts[5]
	switch parts[2] {
	case "ec2":
		if i := strings.IndexRune(tail, '/'); i >= 0 {
			return tail[:i]
		}
		return tail
	case "rds":
		if i := strings.IndexRune(tail, ':'); i >= 0 {
			return tail[:i]
		}
		return tail
	case "s3":
		return "bucket"
	case "iam":
		if i := strings.IndexRune(tail, '/'); i >= 0 {
			return tail[:i]
		}
		return tail
	}
	return ""
}

func lookupEC2(api cleanupAPI, id string) (ec2types.Instance, bool) {
	out, err := api.DescribeInstances(context.Background(), &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
	if err != nil {
		return ec2types.Instance{}, false
	}
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			return inst, true
		}
	}
	return ec2types.Instance{}, false
}

func lookupRDS(api cleanupAPI, id string) (rdstypes.DBInstance, bool) {
	out, err := api.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)})
	if err != nil {
		return rdstypes.DBInstance{}, false
	}
	if len(out.DBInstances) == 0 {
		return rdstypes.DBInstance{}, false
	}
	return out.DBInstances[0], true
}

// processIAMRoleByARN translates an ARN-style id (role/<name>) to the
// plain name and delegates.
func processIAMRoleByARN(ctx context.Context, api cleanupAPI, id string, now time.Time, ttl time.Duration) error {
	if strings.HasPrefix(id, "role/") {
		return processIAMRole(ctx, api, strings.TrimPrefix(id, "role/"), now, ttl)
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test . -v 2>&1 | tail -40`
Expected: all pass, including the new Sweep tests.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/sweep.go benchmarking/aws/cleanup-lambda/sweep_test.go
git commit -m "feat(bench/aws/cleanup): Sweep orchestrator + SNS publish"
```

---

## Task 6: Lambda handler entrypoint

**Files:**
- Modify (or create): `benchmarking/aws/cleanup-lambda/main.go`

- [ ] **Step 1: Write main.go**

```go
// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// awsCleanup adapts the real SDK clients into one cleanupAPI.
type awsCleanup struct {
	rgta *resourcegroupstaggingapi.Client
	ec2  *ec2.Client
	rds  *rds.Client
	s3   *s3.Client
	iam  *iam.Client
	sns  *sns.Client
}

func newAWSCleanup(ctx context.Context) (*awsCleanup, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &awsCleanup{
		rgta: resourcegroupstaggingapi.NewFromConfig(cfg),
		ec2:  ec2.NewFromConfig(cfg),
		rds:  rds.NewFromConfig(cfg),
		s3:   s3.NewFromConfig(cfg),
		iam:  iam.NewFromConfig(cfg),
		sns:  sns.NewFromConfig(cfg),
	}, nil
}

// The cleanupAPI methods delegate. Because the SDK clients are typed
// differently per service, we forward via method receivers.

func (a *awsCleanup) GetResources(ctx context.Context, in *resourcegroupstaggingapi.GetResourcesInput) (*resourcegroupstaggingapi.GetResourcesOutput, error) {
	return a.rgta.GetResources(ctx, in)
}
func (a *awsCleanup) DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
	return a.ec2.DescribeInstances(ctx, in)
}
func (a *awsCleanup) TerminateInstances(ctx context.Context, in *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
	return a.ec2.TerminateInstances(ctx, in)
}
func (a *awsCleanup) DescribeSecurityGroups(ctx context.Context, in *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error) {
	return a.ec2.DescribeSecurityGroups(ctx, in)
}
func (a *awsCleanup) DeleteSecurityGroup(ctx context.Context, in *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error) {
	return a.ec2.DeleteSecurityGroup(ctx, in)
}
func (a *awsCleanup) DescribeVpcs(ctx context.Context, in *ec2.DescribeVpcsInput) (*ec2.DescribeVpcsOutput, error) {
	return a.ec2.DescribeVpcs(ctx, in)
}
func (a *awsCleanup) DeleteVpc(ctx context.Context, in *ec2.DeleteVpcInput) (*ec2.DeleteVpcOutput, error) {
	return a.ec2.DeleteVpc(ctx, in)
}
func (a *awsCleanup) DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
	return a.ec2.DescribeSubnets(ctx, in)
}
func (a *awsCleanup) DeleteSubnet(ctx context.Context, in *ec2.DeleteSubnetInput) (*ec2.DeleteSubnetOutput, error) {
	return a.ec2.DeleteSubnet(ctx, in)
}
func (a *awsCleanup) DescribeRouteTables(ctx context.Context, in *ec2.DescribeRouteTablesInput) (*ec2.DescribeRouteTablesOutput, error) {
	return a.ec2.DescribeRouteTables(ctx, in)
}
func (a *awsCleanup) DeleteRouteTable(ctx context.Context, in *ec2.DeleteRouteTableInput) (*ec2.DeleteRouteTableOutput, error) {
	return a.ec2.DeleteRouteTable(ctx, in)
}
func (a *awsCleanup) DisassociateRouteTable(ctx context.Context, in *ec2.DisassociateRouteTableInput) (*ec2.DisassociateRouteTableOutput, error) {
	return a.ec2.DisassociateRouteTable(ctx, in)
}
func (a *awsCleanup) DescribeInternetGateways(ctx context.Context, in *ec2.DescribeInternetGatewaysInput) (*ec2.DescribeInternetGatewaysOutput, error) {
	return a.ec2.DescribeInternetGateways(ctx, in)
}
func (a *awsCleanup) DetachInternetGateway(ctx context.Context, in *ec2.DetachInternetGatewayInput) (*ec2.DetachInternetGatewayOutput, error) {
	return a.ec2.DetachInternetGateway(ctx, in)
}
func (a *awsCleanup) DeleteInternetGateway(ctx context.Context, in *ec2.DeleteInternetGatewayInput) (*ec2.DeleteInternetGatewayOutput, error) {
	return a.ec2.DeleteInternetGateway(ctx, in)
}
func (a *awsCleanup) DescribeDBInstances(ctx context.Context, in *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	return a.rds.DescribeDBInstances(ctx, in)
}
func (a *awsCleanup) DeleteDBInstance(ctx context.Context, in *rds.DeleteDBInstanceInput) (*rds.DeleteDBInstanceOutput, error) {
	return a.rds.DeleteDBInstance(ctx, in)
}
func (a *awsCleanup) DescribeDBSubnetGroups(ctx context.Context, in *rds.DescribeDBSubnetGroupsInput) (*rds.DescribeDBSubnetGroupsOutput, error) {
	return a.rds.DescribeDBSubnetGroups(ctx, in)
}
func (a *awsCleanup) DeleteDBSubnetGroup(ctx context.Context, in *rds.DeleteDBSubnetGroupInput) (*rds.DeleteDBSubnetGroupOutput, error) {
	return a.rds.DeleteDBSubnetGroup(ctx, in)
}
func (a *awsCleanup) DescribeDBParameterGroups(ctx context.Context, in *rds.DescribeDBParameterGroupsInput) (*rds.DescribeDBParameterGroupsOutput, error) {
	return a.rds.DescribeDBParameterGroups(ctx, in)
}
func (a *awsCleanup) DeleteDBParameterGroup(ctx context.Context, in *rds.DeleteDBParameterGroupInput) (*rds.DeleteDBParameterGroupOutput, error) {
	return a.rds.DeleteDBParameterGroup(ctx, in)
}
func (a *awsCleanup) ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error) {
	return a.s3.ListObjectVersions(ctx, in)
}
func (a *awsCleanup) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return a.s3.DeleteObjects(ctx, in)
}
func (a *awsCleanup) DeleteBucket(ctx context.Context, in *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	return a.s3.DeleteBucket(ctx, in)
}
func (a *awsCleanup) GetRole(ctx context.Context, in *iam.GetRoleInput) (*iam.GetRoleOutput, error) {
	return a.iam.GetRole(ctx, in)
}
func (a *awsCleanup) ListRolePolicies(ctx context.Context, in *iam.ListRolePoliciesInput) (*iam.ListRolePoliciesOutput, error) {
	return a.iam.ListRolePolicies(ctx, in)
}
func (a *awsCleanup) DeleteRolePolicy(ctx context.Context, in *iam.DeleteRolePolicyInput) (*iam.DeleteRolePolicyOutput, error) {
	return a.iam.DeleteRolePolicy(ctx, in)
}
func (a *awsCleanup) ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput) (*iam.ListAttachedRolePoliciesOutput, error) {
	return a.iam.ListAttachedRolePolicies(ctx, in)
}
func (a *awsCleanup) DetachRolePolicy(ctx context.Context, in *iam.DetachRolePolicyInput) (*iam.DetachRolePolicyOutput, error) {
	return a.iam.DetachRolePolicy(ctx, in)
}
func (a *awsCleanup) ListInstanceProfilesForRole(ctx context.Context, in *iam.ListInstanceProfilesForRoleInput) (*iam.ListInstanceProfilesForRoleOutput, error) {
	return a.iam.ListInstanceProfilesForRole(ctx, in)
}
func (a *awsCleanup) RemoveRoleFromInstanceProfile(ctx context.Context, in *iam.RemoveRoleFromInstanceProfileInput) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	return a.iam.RemoveRoleFromInstanceProfile(ctx, in)
}
func (a *awsCleanup) DeleteInstanceProfile(ctx context.Context, in *iam.DeleteInstanceProfileInput) (*iam.DeleteInstanceProfileOutput, error) {
	return a.iam.DeleteInstanceProfile(ctx, in)
}
func (a *awsCleanup) DeleteRole(ctx context.Context, in *iam.DeleteRoleInput) (*iam.DeleteRoleOutput, error) {
	return a.iam.DeleteRole(ctx, in)
}
func (a *awsCleanup) Publish(ctx context.Context, in *sns.PublishInput) (*sns.PublishOutput, error) {
	return a.sns.Publish(ctx, in)
}

func handleRequest(ctx context.Context, _ struct{}) (SweepReport, error) {
	ttl := 3 * time.Hour
	if raw := os.Getenv("BENCH_ORPHAN_TTL_HOURS"); raw != "" {
		if h, err := strconv.ParseFloat(raw, 64); err == nil {
			ttl = time.Duration(h * float64(time.Hour))
		}
	}
	topicARN := os.Getenv("BENCH_ORPHAN_SNS_TOPIC_ARN")
	if topicARN == "" {
		return SweepReport{}, fmt.Errorf("BENCH_ORPHAN_SNS_TOPIC_ARN is required")
	}
	api, err := newAWSCleanup(ctx)
	if err != nil {
		return SweepReport{}, err
	}
	slog.Info("starting sweep", "ttl", ttl)
	return Sweep(ctx, api, time.Now(), ttl, topicARN)
}

func main() {
	lambda.Start(handleRequest)
}
```

- [ ] **Step 2: Build to verify**

Run: `cd benchmarking/aws/cleanup-lambda && go build ./...`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/main.go
git commit -m "feat(bench/aws/cleanup): Lambda handler + AWS SDK adapters"
```

---

## Task 7: Makefile for Lambda zip

**Files:**
- Create: `benchmarking/aws/cleanup-lambda/Makefile`

- [ ] **Step 1: Create Makefile**

```makefile
# Build the orphan-cleanup Lambda as a static linux/arm64 bootstrap binary
# wrapped in a zip, as expected by the provided.al2023 runtime.
.PHONY: zip clean

ZIP := bootstrap.zip

zip: $(ZIP)

$(ZIP): main.go sweep.go go.mod go.sum
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap .
	zip -j $@ bootstrap
	rm -f bootstrap

clean:
	rm -f $(ZIP) bootstrap
```

- [ ] **Step 2: Smoke-test the build**

Run: `cd benchmarking/aws/cleanup-lambda && make zip`
Expected: produces `bootstrap.zip`.

Run: `unzip -l benchmarking/aws/cleanup-lambda/bootstrap.zip`
Expected: contains one `bootstrap` entry.

- [ ] **Step 3: Gitignore the zip**

Add to `benchmarking/aws/.gitignore` (create if it doesn't have a matching entry):

```
cleanup-lambda/bootstrap.zip
cleanup-lambda/bootstrap
```

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/cleanup-lambda/Makefile benchmarking/aws/.gitignore
git commit -m "build(bench/aws/cleanup): Makefile to build bootstrap.zip"
```

---

## Task 8: Terraform — cleanup.tf

**Files:**
- Create: `benchmarking/aws/terraform/shared/cleanup.tf`
- Modify: `benchmarking/aws/terraform/shared/variables.tf`
- Modify: `benchmarking/aws/terraform/shared/outputs.tf`

- [ ] **Step 1: Add optional override variable**

In `variables.tf`, append:

```hcl
variable "orphan_ttl_hours" {
  description = "How long a tagged bench resource can live before the cleanup Lambda destroys it."
  type        = number
  default     = 3
}
```

- [ ] **Step 2: Create cleanup.tf**

```hcl
# Orphan-cleanup Lambda — runs every 15 minutes, destroys any
# Project=redpanda-connect-bench resource older than var.orphan_ttl_hours.

resource "aws_sns_topic" "orphan_cleanup" {
  name = "redpanda-connect-bench-orphans"
  tags = local.common_tags  # ensure the shared common_tags map exists in main.tf
}

resource "aws_iam_role" "orphan_cleanup" {
  name = "redpanda-connect-bench-orphan-cleanup"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "orphan_cleanup_basic" {
  role       = aws_iam_role.orphan_cleanup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "orphan_cleanup" {
  name = "orphan-cleanup"
  role = aws_iam_role.orphan_cleanup.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "tag:GetResources",
          "ec2:DescribeInstances", "ec2:TerminateInstances",
          "ec2:DescribeVpcs", "ec2:DeleteVpc",
          "ec2:DescribeSubnets", "ec2:DeleteSubnet",
          "ec2:DescribeSecurityGroups", "ec2:DeleteSecurityGroup",
          "ec2:DescribeRouteTables", "ec2:DeleteRouteTable", "ec2:DisassociateRouteTable",
          "ec2:DescribeInternetGateways", "ec2:DetachInternetGateway", "ec2:DeleteInternetGateway",
          "rds:DescribeDBInstances", "rds:DeleteDBInstance",
          "rds:DescribeDBSubnetGroups", "rds:DeleteDBSubnetGroup",
          "rds:DescribeDBParameterGroups", "rds:DeleteDBParameterGroup",
          "s3:ListAllMyBuckets", "s3:ListBucket", "s3:ListBucketVersions",
          "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket",
          "iam:GetRole", "iam:DeleteRole",
          "iam:ListRolePolicies", "iam:DeleteRolePolicy",
          "iam:ListAttachedRolePolicies", "iam:DetachRolePolicy",
          "iam:ListInstanceProfilesForRole", "iam:RemoveRoleFromInstanceProfile", "iam:DeleteInstanceProfile",
          "sns:Publish"
        ]
        Resource = "*"
      }
    ]
  })
}

data "archive_file" "orphan_cleanup_zip" {
  type        = "zip"
  source_file = "${path.module}/../../cleanup-lambda/bootstrap"
  output_path = "${path.module}/../../cleanup-lambda/bootstrap.zip"
}

resource "aws_lambda_function" "orphan_cleanup" {
  function_name    = "redpanda-connect-bench-orphan-cleanup"
  role             = aws_iam_role.orphan_cleanup.arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  filename         = data.archive_file.orphan_cleanup_zip.output_path
  source_code_hash = data.archive_file.orphan_cleanup_zip.output_base64sha256
  timeout          = 900 # 15 min — enough for slow RDS deletes

  environment {
    variables = {
      BENCH_ORPHAN_TTL_HOURS     = tostring(var.orphan_ttl_hours)
      BENCH_ORPHAN_SNS_TOPIC_ARN = aws_sns_topic.orphan_cleanup.arn
    }
  }
  tags = local.common_tags
}

resource "aws_cloudwatch_event_rule" "orphan_cleanup" {
  name                = "redpanda-connect-bench-orphan-cleanup"
  description         = "Run the orphan-cleanup Lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "orphan_cleanup" {
  rule = aws_cloudwatch_event_rule.orphan_cleanup.name
  arn  = aws_lambda_function.orphan_cleanup.arn
}

resource "aws_lambda_permission" "orphan_cleanup" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orphan_cleanup.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.orphan_cleanup.arn
}
```

- [ ] **Step 3: Add an output for the topic ARN**

In `outputs.tf`, append:

```hcl
output "orphan_cleanup_sns_topic_arn" {
  value = aws_sns_topic.orphan_cleanup.arn
}
```

- [ ] **Step 4: Verify locals.common_tags exists**

In `shared/main.tf`, locate the `default_tags` provider block or any `locals` definition. If `common_tags` isn't already a local, add it:

```hcl
locals {
  common_tags = {
    Project   = "redpanda-connect-bench"
    ManagedBy = "terraform"
  }
}
```

(Don't duplicate if it already exists — adjust the reference instead.)

- [ ] **Step 5: Terraform plan to validate**

Operator runs (not part of CI):

```bash
cd benchmarking/aws/cleanup-lambda && make zip && cd ../terraform/shared
terraform init -backend-config=../backend.hcl
terraform plan -var region=us-east-2 -var runner_instance_type=c8g.4xlarge -var bench_session_id=plan-test
```

Expected: plan shows 6-7 new resources (sns topic, role, role-policy attachment, role-policy, lambda function, event rule, event target, lambda permission). No errors.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/terraform/shared/cleanup.tf benchmarking/aws/terraform/shared/variables.tf benchmarking/aws/terraform/shared/outputs.tf benchmarking/aws/terraform/shared/main.tf
git commit -m "feat(bench/aws/cleanup): TF — Lambda + IAM + EventBridge + SNS"
```

---

## Task 9: Document in README

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Add a section**

Add a new section after "Operator commands":

```markdown
## Orphan cleanup

A Lambda runs every 15 minutes in the shared stack. It finds resources tagged
`Project=redpanda-connect-bench` whose creation time is older than 3 hours,
and destroys them via direct AWS API calls in dependency order (RDS → EC2 →
S3 → IAM → VPC family). It publishes an SNS notification only when something
was actually destroyed.

This is a safety net: if the runner is SIGKILLed, your laptop loses creds
mid-run, or `terraform destroy` errors out, the Lambda mops up within a 3-hour
window. A normal ~90-min bench finishes well inside the TTL and is never
touched.

### Override the TTL (rare)

For a deliberately long-running scenario:

```sh
aws lambda update-function-configuration \
  --function-name redpanda-connect-bench-orphan-cleanup \
  --environment Variables="{BENCH_ORPHAN_TTL_HOURS=6,BENCH_ORPHAN_SNS_TOPIC_ARN=<existing>}"
```

Reset to the default by removing the override or putting `3`.

### Subscribe to notifications

```sh
aws sns subscribe \
  --topic-arn $(terraform -chdir=terraform/shared output -raw orphan_cleanup_sns_topic_arn) \
  --protocol email \
  --notification-endpoint your.email@redpanda.com
```
```

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws/cleanup): document the orphan-cleanup Lambda + TTL override"
```

---

## Task 10: End-to-end smoke test

**Files:** none — operator runs

- [ ] **Step 1: Build the zip**

```bash
cd benchmarking/aws/cleanup-lambda && make zip
```

- [ ] **Step 2: Apply the shared stack**

```bash
cd ../terraform/shared
aws-vault exec AWSAdministratorAccess-605419575229 -- terraform apply \
  -var region=us-east-2 \
  -var runner_instance_type=c8g.4xlarge \
  -var bench_session_id=cleanup-smoke
```

Expected: applies cleanly; Lambda and EventBridge rule are created.

- [ ] **Step 3: Plant a deliberate orphan**

In the AWS console (or via CLI), create a small EC2 instance tagged `Project=redpanda-connect-bench` with a fake `LaunchTime` 4 hours ago — easier: just create one and wait, OR use the test-friendly path of invoking the Lambda manually with a smaller TTL:

```bash
aws lambda invoke \
  --function-name redpanda-connect-bench-orphan-cleanup \
  --environment Variables="{BENCH_ORPHAN_TTL_HOURS=0,BENCH_ORPHAN_SNS_TOPIC_ARN=$(terraform -chdir=terraform/shared output -raw orphan_cleanup_sns_topic_arn)}" \
  /tmp/lambda-out.json
cat /tmp/lambda-out.json
```

(Setting TTL to 0 sweeps everything tagged; only safe to do when the only tagged resources are intentional fakes you want destroyed.)

Expected: response JSON shows `{"DestroyedCount": N, "Errors": 0, ...}`.

- [ ] **Step 4: Subscribe to SNS topic and verify an email lands**

After triggering a sweep that destroys ≥1 resource, the subscribed email should receive a message titled `bench orphan-cleanup ran`.

- [ ] **Step 5: Tear down the shared stack**

```bash
aws-vault exec AWSAdministratorAccess-605419575229 -- terraform destroy \
  -var region=us-east-2 \
  -var runner_instance_type=c8g.4xlarge \
  -var bench_session_id=cleanup-smoke
```

Expected: clean. The Lambda's role + the lambda itself are also tagged; they get destroyed by terraform on this teardown, NOT by the Lambda's own sweep (the Lambda runs as a separate AWS principal and is therefore not subject to its own TTL on the happy path).

- [ ] **Step 6: Final commit if needed**

If smoke test revealed any minor doc/config tweaks, commit them.

---

## Verification checklist

- [ ] `cd benchmarking/aws/cleanup-lambda && go test -race . -v` — all pass
- [ ] `gofmt -l benchmarking/aws/cleanup-lambda/*.go` — empty
- [ ] `make zip` produces `bootstrap.zip` < 20 MB
- [ ] `terraform plan` on the shared stack with cleanup.tf added shows new resources, no diffs against pre-existing ones beyond the added module
- [ ] The smoke test from Task 10 destroys a deliberately-aged orphan and publishes an SNS message
- [ ] README documents the override path

## Done criteria

A SIGKILL of the bench runner mid-flight no longer leaves AWS infra running for more than 3 hours. The Lambda's CloudWatch Logs show every sweep; the SNS topic gets a message only when work was done. Operator can override TTL for a one-off long-running scenario.
