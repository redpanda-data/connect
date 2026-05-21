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
	"github.com/stretchr/testify/require"
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
)

// FakeAWS satisfies cleanupAPI. Tests pre-populate maps with the resources
// the discovery step should return + each Describe should report, then
// assert which Delete* calls were made.
type FakeAWS struct {
	TaggedResources []rgtatypes.ResourceTagMapping
	EC2Instances    map[string]ec2types.Instance
	DBInstances     map[string]rdstypes.DBInstance
	S3Buckets       map[string]time.Time
	IAMRoles        map[string]time.Time

	// Recorded actions
	Terminated     []string
	DeletedDBs     []string
	DeletedBuckets []string
	DeletedRoles   []string
	DeletedSGs     []string
	SNSMessages    []string
}

func (f *FakeAWS) GetResources(_ context.Context, _ *resourcegroupstaggingapi.GetResourcesInput) (*resourcegroupstaggingapi.GetResourcesOutput, error) {
	return &resourcegroupstaggingapi.GetResourcesOutput{ResourceTagMappingList: f.TaggedResources}, nil
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
	f.Terminated = append(f.Terminated, in.InstanceIds...)
	return &ec2.TerminateInstancesOutput{}, nil
}
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

// Sanity-compile check.
var _ cleanupAPI = (*FakeAWS)(nil)

func TestParseARN_KnownServices(t *testing.T) {
	cases := []struct {
		arn string
		svc string
		id  string
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
