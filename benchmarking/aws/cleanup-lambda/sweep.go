// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"strings"
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

// Sanity reference so the typed packages are not flagged as unused before
// later tasks reference them; remove these blank assignments once the
// referenced types are used by Sweep / FakeAWS / handler in subsequent tasks.
var (
	_ = ec2types.Instance{}
	_ = rdstypes.DBInstance{}
	_ = s3types.Object{}
	_ = iamtypes.Role{}
	_ = rgtatypes.ResourceTagMapping{}
	_ = time.Time{}
)

// parseARN extracts the service code (ec2/rds/s3/iam/...) and the resource
// identifier from an ARN. Resource identifier shapes vary by service:
//
//	ec2 instance:        i-XXX (the part after instance/)
//	rds db instance:     the DBInstanceIdentifier (after `db:`)
//	s3 bucket:           the bucket name
//	iam role/profile:    role/<name> or instance-profile/<name>
func parseARN(arn string) (service, resourceID string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return "", "", false
	}
	service = parts[2]
	tail := parts[5]
	switch service {
	case "ec2":
		// "instance/i-abc" -> "i-abc"; "vpc/vpc-abc" -> "vpc-abc"
		i := strings.LastIndex(tail, "/")
		if i >= 0 {
			return service, tail[i+1:], true
		}
		return service, tail, true
	case "rds":
		// "db:rpcn-bench-pg-pg" / "subgrp:rpcn-..."
		i := strings.LastIndex(tail, ":")
		if i >= 0 {
			return service, tail[i+1:], true
		}
		return service, tail, true
	case "s3":
		return service, tail, true
	case "iam":
		// keep the role/ prefix so callers know the type
		return service, tail, true
	default:
		return service, tail, true
	}
}

func olderThanTTL(t, now time.Time, ttl time.Duration) bool {
	return now.Sub(t) > ttl
}

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
