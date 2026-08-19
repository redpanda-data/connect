// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"strconv"
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
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/require"
)

// s3VersionPage is one page of a faked ListObjectVersions response.
type s3VersionPage struct {
	Versions            []s3types.ObjectVersion
	DeleteMarkers       []s3types.DeleteMarkerEntry
	IsTruncated         bool
	NextKeyMarker       *string
	NextVersionIdMarker *string
}

// FakeAWS satisfies cleanupAPI. Tests pre-populate maps with the resources
// the discovery step should return + each Describe should report, then
// assert which Delete* calls were made.
type FakeAWS struct {
	TaggedResources []rgtatypes.ResourceTagMapping
	// TaggedPages, when non-empty, serves GetResources as a paginated
	// sequence instead of a single page from TaggedResources.
	TaggedPages      [][]rgtatypes.ResourceTagMapping
	EC2Instances     map[string]ec2types.Instance
	DBInstances      map[string]rdstypes.DBInstance
	S3Buckets        map[string]time.Time
	S3VersionPages   map[string][]s3VersionPage
	IAMRoles         map[string]time.Time
	RouteTables      map[string]ec2types.RouteTable
	InternetGateways map[string]ec2types.InternetGateway
	SecurityGroups   map[string]ec2types.SecurityGroup
	// FailOn maps "<Method>:<id>" to an error that method call returns
	// instead of succeeding, so tests can exercise the errored-delete path.
	FailOn map[string]error

	// Recorded actions
	Terminated             []string
	DeletedDBs             []string
	DeletedBuckets         []string
	DeletedRoles           []string
	DeletedSGs             []string
	DeletedVPCs            []string
	DeletedSubnets         []string
	DeletedRouteTables     []string
	Disassociated          []string
	DeletedIGWs            []string
	DetachedIGWs           []string
	DeletedRDSSubnetGroups []string
	DeletedRDSParamGroups  []string
	SNSMessages            []string
	// CallLog records "<Method>:<id>" for every mutating call, in call
	// order, so tests can assert cross-resource dependency ordering.
	CallLog []string

	// ListObjectVersionsRequests records every request, in order, so tests
	// can assert the key/version marker propagation between pages.
	ListObjectVersionsRequests []s3.ListObjectVersionsInput
	// SeenCtx records the last context.Context observed by GetResources, so
	// tests can assert Sweep threads the caller's ctx through rather than
	// substituting context.Background().
	SeenCtx context.Context
}

func (f *FakeAWS) failIfConfigured(call, id string) error {
	return f.FailOn[call+":"+id]
}

func (f *FakeAWS) GetResources(ctx context.Context, in *resourcegroupstaggingapi.GetResourcesInput) (*resourcegroupstaggingapi.GetResourcesOutput, error) {
	f.SeenCtx = ctx
	if len(f.TaggedPages) == 0 {
		return &resourcegroupstaggingapi.GetResourcesOutput{ResourceTagMappingList: f.TaggedResources}, nil
	}
	idx := 0
	if tok := aws.ToString(in.PaginationToken); tok != "" {
		parsed, err := strconv.Atoi(tok)
		if err != nil {
			return nil, err
		}
		idx = parsed
	}
	out := &resourcegroupstaggingapi.GetResourcesOutput{ResourceTagMappingList: f.TaggedPages[idx]}
	if idx+1 < len(f.TaggedPages) {
		out.PaginationToken = aws.String(strconv.Itoa(idx + 1))
	}
	return out, nil
}

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
	id := in.InstanceIds[0]
	if err := f.failIfConfigured("TerminateInstances", id); err != nil {
		return nil, err
	}
	f.Terminated = append(f.Terminated, in.InstanceIds...)
	return &ec2.TerminateInstancesOutput{}, nil
}

func (f *FakeAWS) DescribeSecurityGroups(_ context.Context, in *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error) {
	out := &ec2.DescribeSecurityGroupsOutput{}
	for _, id := range in.GroupIds {
		if sg, ok := f.SecurityGroups[id]; ok {
			out.SecurityGroups = append(out.SecurityGroups, sg)
		}
	}
	return out, nil
}

func (f *FakeAWS) DeleteSecurityGroup(_ context.Context, in *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error) {
	id := aws.ToString(in.GroupId)
	if err := f.failIfConfigured("DeleteSecurityGroup", id); err != nil {
		return nil, err
	}
	f.DeletedSGs = append(f.DeletedSGs, id)
	f.CallLog = append(f.CallLog, "DeleteSecurityGroup:"+id)
	return &ec2.DeleteSecurityGroupOutput{}, nil
}

func (f *FakeAWS) DescribeVpcs(_ context.Context, _ *ec2.DescribeVpcsInput) (*ec2.DescribeVpcsOutput, error) {
	return &ec2.DescribeVpcsOutput{}, nil
}

func (f *FakeAWS) DeleteVpc(_ context.Context, in *ec2.DeleteVpcInput) (*ec2.DeleteVpcOutput, error) {
	id := aws.ToString(in.VpcId)
	if err := f.failIfConfigured("DeleteVpc", id); err != nil {
		return nil, err
	}
	f.DeletedVPCs = append(f.DeletedVPCs, id)
	f.CallLog = append(f.CallLog, "DeleteVpc:"+id)
	return &ec2.DeleteVpcOutput{}, nil
}

func (f *FakeAWS) DescribeSubnets(_ context.Context, _ *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
	return &ec2.DescribeSubnetsOutput{}, nil
}

func (f *FakeAWS) DeleteSubnet(_ context.Context, in *ec2.DeleteSubnetInput) (*ec2.DeleteSubnetOutput, error) {
	id := aws.ToString(in.SubnetId)
	if err := f.failIfConfigured("DeleteSubnet", id); err != nil {
		return nil, err
	}
	f.DeletedSubnets = append(f.DeletedSubnets, id)
	f.CallLog = append(f.CallLog, "DeleteSubnet:"+id)
	return &ec2.DeleteSubnetOutput{}, nil
}

func (f *FakeAWS) DescribeRouteTables(_ context.Context, in *ec2.DescribeRouteTablesInput) (*ec2.DescribeRouteTablesOutput, error) {
	out := &ec2.DescribeRouteTablesOutput{}
	for _, id := range in.RouteTableIds {
		if rt, ok := f.RouteTables[id]; ok {
			out.RouteTables = append(out.RouteTables, rt)
		}
	}
	return out, nil
}

func (f *FakeAWS) DeleteRouteTable(_ context.Context, in *ec2.DeleteRouteTableInput) (*ec2.DeleteRouteTableOutput, error) {
	id := aws.ToString(in.RouteTableId)
	if err := f.failIfConfigured("DeleteRouteTable", id); err != nil {
		return nil, err
	}
	f.DeletedRouteTables = append(f.DeletedRouteTables, id)
	f.CallLog = append(f.CallLog, "DeleteRouteTable:"+id)
	return &ec2.DeleteRouteTableOutput{}, nil
}

func (f *FakeAWS) DisassociateRouteTable(_ context.Context, in *ec2.DisassociateRouteTableInput) (*ec2.DisassociateRouteTableOutput, error) {
	id := aws.ToString(in.AssociationId)
	f.Disassociated = append(f.Disassociated, id)
	f.CallLog = append(f.CallLog, "DisassociateRouteTable:"+id)
	return &ec2.DisassociateRouteTableOutput{}, nil
}

func (f *FakeAWS) DescribeInternetGateways(_ context.Context, in *ec2.DescribeInternetGatewaysInput) (*ec2.DescribeInternetGatewaysOutput, error) {
	out := &ec2.DescribeInternetGatewaysOutput{}
	for _, id := range in.InternetGatewayIds {
		if igw, ok := f.InternetGateways[id]; ok {
			out.InternetGateways = append(out.InternetGateways, igw)
		}
	}
	return out, nil
}

func (f *FakeAWS) DetachInternetGateway(_ context.Context, in *ec2.DetachInternetGatewayInput) (*ec2.DetachInternetGatewayOutput, error) {
	id := aws.ToString(in.InternetGatewayId) + "/" + aws.ToString(in.VpcId)
	f.DetachedIGWs = append(f.DetachedIGWs, id)
	f.CallLog = append(f.CallLog, "DetachInternetGateway:"+id)
	return &ec2.DetachInternetGatewayOutput{}, nil
}

func (f *FakeAWS) DeleteInternetGateway(_ context.Context, in *ec2.DeleteInternetGatewayInput) (*ec2.DeleteInternetGatewayOutput, error) {
	id := aws.ToString(in.InternetGatewayId)
	if err := f.failIfConfigured("DeleteInternetGateway", id); err != nil {
		return nil, err
	}
	f.DeletedIGWs = append(f.DeletedIGWs, id)
	f.CallLog = append(f.CallLog, "DeleteInternetGateway:"+id)
	return &ec2.DeleteInternetGatewayOutput{}, nil
}

func (f *FakeAWS) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput) (*rds.DescribeDBInstancesOutput, error) {
	out := &rds.DescribeDBInstancesOutput{}
	id := aws.ToString(in.DBInstanceIdentifier)
	if db, ok := f.DBInstances[id]; ok {
		out.DBInstances = []rdstypes.DBInstance{db}
	}
	return out, nil
}

func (f *FakeAWS) DeleteDBInstance(_ context.Context, in *rds.DeleteDBInstanceInput) (*rds.DeleteDBInstanceOutput, error) {
	id := aws.ToString(in.DBInstanceIdentifier)
	if err := f.failIfConfigured("DeleteDBInstance", id); err != nil {
		return nil, err
	}
	f.DeletedDBs = append(f.DeletedDBs, id)
	return &rds.DeleteDBInstanceOutput{}, nil
}

func (f *FakeAWS) DescribeDBSubnetGroups(_ context.Context, _ *rds.DescribeDBSubnetGroupsInput) (*rds.DescribeDBSubnetGroupsOutput, error) {
	return &rds.DescribeDBSubnetGroupsOutput{}, nil
}

func (f *FakeAWS) DeleteDBSubnetGroup(_ context.Context, in *rds.DeleteDBSubnetGroupInput) (*rds.DeleteDBSubnetGroupOutput, error) {
	name := aws.ToString(in.DBSubnetGroupName)
	if err := f.failIfConfigured("DeleteDBSubnetGroup", name); err != nil {
		return nil, err
	}
	f.DeletedRDSSubnetGroups = append(f.DeletedRDSSubnetGroups, name)
	return &rds.DeleteDBSubnetGroupOutput{}, nil
}

func (f *FakeAWS) DescribeDBParameterGroups(_ context.Context, _ *rds.DescribeDBParameterGroupsInput) (*rds.DescribeDBParameterGroupsOutput, error) {
	return &rds.DescribeDBParameterGroupsOutput{}, nil
}

func (f *FakeAWS) DeleteDBParameterGroup(_ context.Context, in *rds.DeleteDBParameterGroupInput) (*rds.DeleteDBParameterGroupOutput, error) {
	name := aws.ToString(in.DBParameterGroupName)
	if err := f.failIfConfigured("DeleteDBParameterGroup", name); err != nil {
		return nil, err
	}
	f.DeletedRDSParamGroups = append(f.DeletedRDSParamGroups, name)
	return &rds.DeleteDBParameterGroupOutput{}, nil
}

func (f *FakeAWS) ListBuckets(_ context.Context, _ *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	out := &s3.ListBucketsOutput{}
	for name, created := range f.S3Buckets {
		t := created
		n := name
		out.Buckets = append(out.Buckets, s3types.Bucket{Name: &n, CreationDate: &t})
	}
	return out, nil
}

func (f *FakeAWS) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error) {
	bucket := aws.ToString(in.Bucket)
	f.ListObjectVersionsRequests = append(f.ListObjectVersionsRequests, *in)

	pages := f.S3VersionPages[bucket]
	if len(pages) == 0 {
		return &s3.ListObjectVersionsOutput{}, nil
	}
	// The index for this call is how many prior requests targeted this
	// bucket (the just-recorded one included).
	idx := -1
	for _, r := range f.ListObjectVersionsRequests {
		if aws.ToString(r.Bucket) == bucket {
			idx++
		}
	}
	if idx >= len(pages) {
		idx = len(pages) - 1
	}
	p := pages[idx]
	out := &s3.ListObjectVersionsOutput{
		Versions:      p.Versions,
		DeleteMarkers: p.DeleteMarkers,
	}
	if p.IsTruncated {
		out.IsTruncated = aws.Bool(true)
		out.NextKeyMarker = p.NextKeyMarker
		out.NextVersionIdMarker = p.NextVersionIdMarker
	}
	return out, nil
}

func (f *FakeAWS) DeleteObjects(_ context.Context, _ *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{}, nil
}

func (f *FakeAWS) DeleteBucket(_ context.Context, in *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	id := aws.ToString(in.Bucket)
	if err := f.failIfConfigured("DeleteBucket", id); err != nil {
		return nil, err
	}
	f.DeletedBuckets = append(f.DeletedBuckets, id)
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
	name := aws.ToString(in.RoleName)
	if err := f.failIfConfigured("DeleteRole", name); err != nil {
		return nil, err
	}
	f.DeletedRoles = append(f.DeletedRoles, name)
	return &iam.DeleteRoleOutput{}, nil
}

func (f *FakeAWS) Publish(_ context.Context, in *sns.PublishInput) (*sns.PublishOutput, error) {
	f.SNSMessages = append(f.SNSMessages, aws.ToString(in.Message))
	return &sns.PublishOutput{}, nil
}

// Sanity-compile check.
var _ cleanupAPI = (*FakeAWS)(nil)

// sessionTag builds the bench-session-id tag Sweep decodes for age-checking
// resources (VPCs, subnets, ...) that carry no native creation-time field.
func sessionTag(ts time.Time) []rgtatypes.Tag {
	return []rgtatypes.Tag{{
		Key:   aws.String(benchSessionTagKey),
		Value: aws.String("bench-" + ts.Format(sessionIDTimeLayout)),
	}}
}

func TestParseARN_KnownServices(t *testing.T) {
	cases := []struct {
		arn string
		svc string
		id  string
	}{
		{"arn:aws:ec2:us-east-2:605419575229:instance/i-abc123", "ec2", "i-abc123"},
		{"arn:aws:ec2:us-east-2:605419575229:vpc/vpc-abc123", "ec2", "vpc-abc123"},
		{"arn:aws:rds:us-east-2:605419575229:db:rpcn-bench-pg-pg", "rds", "rpcn-bench-pg-pg"},
		{"arn:aws:rds:us-east-2:605419575229:subgrp:rpcn-bench-pg-subnets", "rds", "rpcn-bench-pg-subnets"},
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
	destroyedOld, err := processEC2Instance(context.Background(), api, "i-old", now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyedOld)
	destroyedYoung, err := processEC2Instance(context.Background(), api, "i-young", now, 3*time.Hour)
	require.NoError(t, err)
	require.False(t, destroyedYoung)
	require.Equal(t, []string{"i-old"}, api.Terminated)
}

func TestProcessEC2Instance_TerminateErrorNotDestroyed(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	boom := errAssertion("boom")
	api := &FakeAWS{
		EC2Instances: map[string]ec2types.Instance{
			"i-old": {InstanceId: aws.String("i-old"), LaunchTime: &old},
		},
		FailOn: map[string]error{"TerminateInstances:i-old": boom},
	}
	destroyed, err := processEC2Instance(context.Background(), api, "i-old", now, 3*time.Hour)
	require.ErrorIs(t, err, boom)
	require.False(t, destroyed)
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
	destroyedOld, err := processRDSInstance(context.Background(), api, "old-db", now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyedOld)
	destroyedYoung, err := processRDSInstance(context.Background(), api, "young-db", now, 3*time.Hour)
	require.NoError(t, err)
	require.False(t, destroyedYoung)
	require.Equal(t, []string{"old-db"}, api.DeletedDBs)
}

func TestProcessS3Bucket_OldEmptyAndDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)

	api := &FakeAWS{
		S3Buckets: map[string]time.Time{"rpcn-bench-results-old": old},
	}
	destroyed, err := processS3Bucket(context.Background(), api, "rpcn-bench-results-old", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Equal(t, []string{"rpcn-bench-results-old"}, api.DeletedBuckets)
}

// TestProcessS3Bucket_YoungNotDeleted is a regression test for the bug where
// production skipped the TTL check (no s3AgeProvider type assertion match)
// and unconditionally deleted any tagged S3 bucket.
func TestProcessS3Bucket_YoungNotDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	young := now.Add(-30 * time.Minute)

	api := &FakeAWS{
		S3Buckets: map[string]time.Time{"rpcn-bench-results-young": young},
	}
	destroyed, err := processS3Bucket(context.Background(), api, "rpcn-bench-results-young", young, now, 3*time.Hour)
	require.NoError(t, err)
	require.False(t, destroyed)
	require.Empty(t, api.DeletedBuckets, "young bucket must NOT be deleted")
}

// TestProcessS3Bucket_VersionMarkerPagination is a regression test for the
// bug where only NextKeyMarker was carried into the next ListObjectVersions
// request. Dropping NextVersionIdMarker restarts version listing for the
// marker key from its first version on every page, so remaining versions of
// that key are silently skipped and DeleteBucket fails with BucketNotEmpty.
func TestProcessS3Bucket_VersionMarkerPagination(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	bucket := "rpcn-bench-results-paginated"

	api := &FakeAWS{
		S3Buckets: map[string]time.Time{bucket: old},
		S3VersionPages: map[string][]s3VersionPage{
			bucket: {
				{
					Versions:            []s3types.ObjectVersion{{Key: aws.String("k1"), VersionId: aws.String("v1")}},
					IsTruncated:         true,
					NextKeyMarker:       aws.String("k1"),
					NextVersionIdMarker: aws.String("v1"),
				},
				{
					// Second version of the SAME key k1: only reachable if
					// the version marker from page 1 was actually sent.
					Versions: []s3types.ObjectVersion{{Key: aws.String("k1"), VersionId: aws.String("v2")}},
				},
			},
		},
	}
	destroyed, err := processS3Bucket(context.Background(), api, bucket, old, now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Len(t, api.ListObjectVersionsRequests, 2)
	second := api.ListObjectVersionsRequests[1]
	require.Equal(t, "k1", aws.ToString(second.KeyMarker), "second page must carry the key marker forward")
	require.Equal(t, "v1", aws.ToString(second.VersionIdMarker), "second page must carry the version-id marker forward")
	require.Equal(t, []string{bucket}, api.DeletedBuckets)
}

func TestProcessIAMRole_OldGetsDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		IAMRoles: map[string]time.Time{"rpcn-bench-host-old": old},
	}
	destroyed, err := processIAMRole(context.Background(), api, "rpcn-bench-host-old", now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Equal(t, []string{"rpcn-bench-host-old"}, api.DeletedRoles)
}

func TestProcessRouteTable_MainSkipped(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		RouteTables: map[string]ec2types.RouteTable{
			"rtb-main": {
				RouteTableId: aws.String("rtb-main"),
				Associations: []ec2types.RouteTableAssociation{{Main: aws.Bool(true)}},
			},
		},
	}
	destroyed, err := processRouteTable(context.Background(), api, "rtb-main", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.False(t, destroyed)
	require.Empty(t, api.DeletedRouteTables)
}

func TestProcessRouteTable_NonMainDisassociatedAndDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		RouteTables: map[string]ec2types.RouteTable{
			"rtb-1": {
				RouteTableId: aws.String("rtb-1"),
				Associations: []ec2types.RouteTableAssociation{
					{Main: aws.Bool(false), RouteTableAssociationId: aws.String("rtbassoc-1")},
				},
			},
		},
	}
	destroyed, err := processRouteTable(context.Background(), api, "rtb-1", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Equal(t, []string{"rtbassoc-1"}, api.Disassociated)
	require.Equal(t, []string{"rtb-1"}, api.DeletedRouteTables)
}

func TestProcessInternetGateway_DetachedThenDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		InternetGateways: map[string]ec2types.InternetGateway{
			"igw-1": {
				InternetGatewayId: aws.String("igw-1"),
				Attachments:       []ec2types.InternetGatewayAttachment{{VpcId: aws.String("vpc-1")}},
			},
		},
	}
	destroyed, err := processInternetGateway(context.Background(), api, "igw-1", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Equal(t, []string{"igw-1/vpc-1"}, api.DetachedIGWs)
	require.Equal(t, []string{"igw-1"}, api.DeletedIGWs)
}

func TestProcessSecurityGroup_DefaultSkipped(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		SecurityGroups: map[string]ec2types.SecurityGroup{
			"sg-default": {GroupId: aws.String("sg-default"), GroupName: aws.String("default")},
		},
	}
	destroyed, err := processSecurityGroup(context.Background(), api, "sg-default", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.False(t, destroyed)
	require.Empty(t, api.DeletedSGs)
}

func TestProcessSecurityGroup_NonDefaultDeleted(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		SecurityGroups: map[string]ec2types.SecurityGroup{
			"sg-1": {GroupId: aws.String("sg-1"), GroupName: aws.String("rpcn-bench-sg")},
		},
	}
	destroyed, err := processSecurityGroup(context.Background(), api, "sg-1", old, now, 3*time.Hour)
	require.NoError(t, err)
	require.True(t, destroyed)
	require.Equal(t, []string{"sg-1"}, api.DeletedSGs)
}

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

// TestSweep_GetResourcesPagination is a regression test for the bug where
// Sweep only read the first page of GetResources (the API caps at 100
// results per page) and silently ignored the rest.
func TestSweep_GetResourcesPagination(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		TaggedPages: [][]rgtatypes.ResourceTagMapping{
			{{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:instance/i-page1")}},
			{{ResourceARN: aws.String("arn:aws:rds:us-east-2:1:db:page2-db")}},
		},
		EC2Instances: map[string]ec2types.Instance{
			"i-page1": {InstanceId: aws.String("i-page1"), LaunchTime: &old},
		},
		DBInstances: map[string]rdstypes.DBInstance{
			"page2-db": {DBInstanceIdentifier: aws.String("page2-db"), InstanceCreateTime: &old},
		},
	}
	report, err := Sweep(context.Background(), api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.Equal(t, []string{"i-page1"}, api.Terminated, "second page must be fetched via PaginationToken")
	require.Equal(t, []string{"page2-db"}, api.DeletedDBs, "second page must be fetched via PaginationToken")
	require.Equal(t, 2, report.DestroyedCount)
}

// TestSweep_ErroredDeleteNotReportedDestroyed is a regression test for the
// bug where Destroyed/DestroyedCount were derived purely from resource age,
// so a resource whose delete call errored was still reported (and
// SNS-alerted) as destroyed.
func TestSweep_ErroredDeleteNotReportedDestroyed(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	api := &FakeAWS{
		TaggedResources: []rgtatypes.ResourceTagMapping{
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:instance/i-old")},
		},
		EC2Instances: map[string]ec2types.Instance{
			"i-old": {InstanceId: aws.String("i-old"), LaunchTime: &old},
		},
		FailOn: map[string]error{"TerminateInstances:i-old": errAssertion("boom")},
	}
	report, err := Sweep(context.Background(), api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.Equal(t, 0, report.DestroyedCount)
	require.Empty(t, report.Destroyed)
	require.Equal(t, 1, report.Errors)
	require.Empty(t, api.SNSMessages, "must not publish when nothing actually got destroyed")
}

// TestSweep_ThreadsCallerContext is a regression test for lookupEC2/
// lookupRDS having used context.Background() instead of the caller's ctx.
func TestSweep_ThreadsCallerContext(t *testing.T) {
	type ctxKey struct{}
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	api := &FakeAWS{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")

	_, err := Sweep(ctx, api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.NotNil(t, api.SeenCtx)
	require.NotEqual(t, context.Background(), api.SeenCtx, "Sweep must thread the caller's ctx through, not substitute context.Background()")
	require.Equal(t, "marker", api.SeenCtx.Value(ctxKey{}))
}

// TestSweep_FullStrandedStack exercises a realistic botched-teardown leak:
// instances/RDS already gone, but the VPC chain (route tables, IGW, subnet,
// security groups, VPC) plus RDS subnet/parameter groups are still there.
// It asserts every resource type is destroyed and that dependency order is
// respected: RDS subnet/parameter groups and route-table disassociation
// before route-table deletion, IGW detach before IGW delete, and the VPC
// deleted only after everything inside it.
func TestSweep_FullStrandedStack(t *testing.T) {
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)
	old := now.Add(-4 * time.Hour)
	oldTag := sessionTag(old)

	api := &FakeAWS{
		TaggedResources: []rgtatypes.ResourceTagMapping{
			{ResourceARN: aws.String("arn:aws:rds:us-east-2:1:subgrp:rpcn-bench-subnets"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:rds:us-east-2:1:pg:rpcn-bench-params"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:route-table/rtb-1"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:internet-gateway/igw-1"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:subnet/subnet-1"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:security-group/sg-1"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:security-group/sg-default"), Tags: oldTag},
			{ResourceARN: aws.String("arn:aws:ec2:us-east-2:1:vpc/vpc-1"), Tags: oldTag},
		},
		RouteTables: map[string]ec2types.RouteTable{
			"rtb-1": {
				RouteTableId: aws.String("rtb-1"),
				Associations: []ec2types.RouteTableAssociation{
					{Main: aws.Bool(false), RouteTableAssociationId: aws.String("rtbassoc-1")},
				},
			},
		},
		InternetGateways: map[string]ec2types.InternetGateway{
			"igw-1": {
				InternetGatewayId: aws.String("igw-1"),
				Attachments:       []ec2types.InternetGatewayAttachment{{VpcId: aws.String("vpc-1")}},
			},
		},
		SecurityGroups: map[string]ec2types.SecurityGroup{
			"sg-1":       {GroupId: aws.String("sg-1"), GroupName: aws.String("rpcn-bench-sg")},
			"sg-default": {GroupId: aws.String("sg-default"), GroupName: aws.String("default")},
		},
	}

	report, err := Sweep(context.Background(), api, now, 3*time.Hour, "arn:sns:topic")
	require.NoError(t, err)
	require.Equal(t, 0, report.Errors)

	require.Equal(t, []string{"rpcn-bench-subnets"}, api.DeletedRDSSubnetGroups)
	require.Equal(t, []string{"rpcn-bench-params"}, api.DeletedRDSParamGroups)
	require.Equal(t, []string{"rtbassoc-1"}, api.Disassociated)
	require.Equal(t, []string{"rtb-1"}, api.DeletedRouteTables)
	require.Equal(t, []string{"igw-1/vpc-1"}, api.DetachedIGWs)
	require.Equal(t, []string{"igw-1"}, api.DeletedIGWs)
	require.Equal(t, []string{"subnet-1"}, api.DeletedSubnets)
	require.Equal(t, []string{"sg-1"}, api.DeletedSGs, "the default SG must never be deleted")
	require.Equal(t, []string{"vpc-1"}, api.DeletedVPCs)

	// Dependency order, read off the single cross-resource CallLog:
	// disassociate before route-table delete, detach before IGW delete,
	// and the VPC only after its route table, IGW, subnet and non-default
	// security group are gone.
	disassocIdx := indexOfCall(api.CallLog, "DisassociateRouteTable:rtbassoc-1")
	rtbDeleteIdx := indexOfCall(api.CallLog, "DeleteRouteTable:rtb-1")
	require.Less(t, disassocIdx, rtbDeleteIdx)

	detachIdx := indexOfCall(api.CallLog, "DetachInternetGateway:igw-1/vpc-1")
	igwDeleteIdx := indexOfCall(api.CallLog, "DeleteInternetGateway:igw-1")
	require.Less(t, detachIdx, igwDeleteIdx)

	vpcDeleteIdx := indexOfCall(api.CallLog, "DeleteVpc:vpc-1")
	require.Greater(t, vpcDeleteIdx, rtbDeleteIdx)
	require.Greater(t, vpcDeleteIdx, igwDeleteIdx)
	require.Greater(t, vpcDeleteIdx, indexOfCall(api.CallLog, "DeleteSubnet:subnet-1"))
	require.Greater(t, vpcDeleteIdx, indexOfCall(api.CallLog, "DeleteSecurityGroup:sg-1"))

	require.Equal(t, 7, report.DestroyedCount, "the skipped default SG must not count toward Destroyed")
}

func indexOfCall(calls []string, want string) int {
	for i, c := range calls {
		if c == want {
			return i
		}
	}
	return -1
}

// errAssertion is a minimal error type for FailOn fixtures.
type errAssertion string

func (e errAssertion) Error() string { return string(e) }
