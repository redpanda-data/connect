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
func (a *awsCleanup) ListBuckets(ctx context.Context, in *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return a.s3.ListBuckets(ctx, in)
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
