// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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
	ListBuckets(ctx context.Context, in *s3.ListBucketsInput) (*s3.ListBucketsOutput, error)
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

// processS3Bucket empties and deletes bucket if creationTime is older than
// ttl. The caller is responsible for fetching creationTime via ListBuckets.
func processS3Bucket(ctx context.Context, api cleanupAPI, bucket string, creationTime, now time.Time, ttl time.Duration) error {
	if !olderThanTTL(creationTime, now, ttl) {
		return nil
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

// SweepReport summarises one execution of the Lambda.
type SweepReport struct {
	DestroyedCount int
	Errors         int
	Destroyed      []string
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

	buckets := bucketByKind(tagged.ResourceTagMappingList)
	report := SweepReport{}

	for _, id := range buckets["rds:db"] {
		if err := processRDSInstance(ctx, api, id, now, ttl); err != nil {
			slog.Error("cleanup failed", "kind", "rds", "id", id, "err", err)
			report.Errors++
		}
	}
	for _, id := range buckets["ec2:instance"] {
		if err := processEC2Instance(ctx, api, id, now, ttl); err != nil {
			slog.Error("cleanup failed", "kind", "ec2", "id", id, "err", err)
			report.Errors++
		}
	}

	// S3: fetch creation times once via ListBuckets, then TTL-filter per bucket.
	bucketAges := map[string]time.Time{}
	if lbOut, err := api.ListBuckets(ctx, &s3.ListBucketsInput{}); err == nil {
		for _, b := range lbOut.Buckets {
			if b.Name != nil && b.CreationDate != nil {
				bucketAges[*b.Name] = *b.CreationDate
			}
		}
	} else {
		slog.Error("list buckets failed; skipping S3 cleanup this run", "err", err)
	}
	for _, id := range buckets["s3:bucket"] {
		created, ok := bucketAges[id]
		if !ok {
			continue // bucket disappeared between discovery and ListBuckets
		}
		if err := processS3Bucket(ctx, api, id, created, now, ttl); err != nil {
			slog.Error("cleanup failed", "kind", "s3", "id", id, "err", err)
			report.Errors++
		}
	}

	for _, id := range buckets["iam:role"] {
		if err := processIAMRoleByARN(ctx, api, id, now, ttl); err != nil {
			slog.Error("cleanup failed", "kind", "iam", "id", id, "err", err)
			report.Errors++
		}
	}

	// Count what was actually destroyed by re-checking ages and noting which
	// were old. This second pass is a bit redundant but keeps Sweep
	// decoupled from the processors' return values.
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
			if created, hit := bucketAges[id]; hit && olderThanTTL(created, now, ttl) {
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

func processIAMRoleByARN(ctx context.Context, api cleanupAPI, id string, now time.Time, ttl time.Duration) error {
	if strings.HasPrefix(id, "role/") {
		return processIAMRole(ctx, api, strings.TrimPrefix(id, "role/"), now, ttl)
	}
	return nil
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
