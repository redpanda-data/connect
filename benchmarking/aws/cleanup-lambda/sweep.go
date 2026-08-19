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
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/rds"
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
//	ec2 instance/vpc/subnet/...: i-XXX, vpc-XXX, ... (the part after the "/")
//	rds db instance/subgrp/pg:  the identifier (after the ":")
//	s3 bucket:                  the bucket name
//	iam role/profile:           role/<name> or instance-profile/<name>
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
		// "db:rpcn-bench-pg-pg" / "subgrp:rpcn-..." / "pg:rpcn-..."
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

// benchSessionTagKey/sessionIDTimeLayout decode the creation timestamp
// embedded in the "bench-session-id" tag (applied via default_tags in
// terraform/shared/main.tf; value format "bench-YYYYMMDD-HHMMSS" comes from
// the runner's newSessionID). VPC-level resources (VPC, subnet, route
// table, internet gateway, security group, RDS subnet/parameter group)
// carry no creation-time field in their own Describe response, so this tag
// is the only age signal available for them.
const (
	benchSessionTagKey  = "bench-session-id"
	sessionIDTimeLayout = "20060102-150405"
)

func sessionCreatedAt(m rgtatypes.ResourceTagMapping) (time.Time, bool) {
	for _, t := range m.Tags {
		if aws.ToString(t.Key) != benchSessionTagKey {
			continue
		}
		v := strings.TrimPrefix(aws.ToString(t.Value), "bench-")
		ts, err := time.Parse(sessionIDTimeLayout, v)
		if err != nil {
			return time.Time{}, false
		}
		return ts, true
	}
	return time.Time{}, false
}

// resourceCreatedAt maps each tagged resource's identifier (as returned by
// parseARN) to the creation time decoded from its bench-session-id tag.
// Resources without a decodable tag are omitted.
func resourceCreatedAt(mappings []rgtatypes.ResourceTagMapping) map[string]time.Time {
	out := map[string]time.Time{}
	for _, m := range mappings {
		_, id, ok := parseARN(aws.ToString(m.ResourceARN))
		if !ok {
			continue
		}
		if ts, found := sessionCreatedAt(m); found {
			out[id] = ts
		}
	}
	return out
}

func processEC2Instance(ctx context.Context, api cleanupAPI, instanceID string, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	out, err := api.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	if err != nil {
		return false, err
	}
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			if inst.LaunchTime == nil {
				continue
			}
			if !olderThanTTL(*inst.LaunchTime, now, ttl) {
				return false, nil
			}
			if _, err := api.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{instanceID}}); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func processRDSInstance(ctx context.Context, api cleanupAPI, dbID string, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	out, err := api.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(dbID)})
	if err != nil {
		return false, err
	}
	for _, db := range out.DBInstances {
		if db.InstanceCreateTime == nil {
			continue
		}
		if !olderThanTTL(*db.InstanceCreateTime, now, ttl) {
			return false, nil
		}
		if _, err := api.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:   aws.String(dbID),
			SkipFinalSnapshot:      aws.Bool(true),
			DeleteAutomatedBackups: aws.Bool(true),
		}); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// processS3Bucket empties and deletes bucket if creationTime is older than
// ttl. The caller is responsible for fetching creationTime via ListBuckets.
func processS3Bucket(ctx context.Context, api cleanupAPI, bucket string, creationTime, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(creationTime, now, ttl) {
		return false, nil
	}

	// Empty the bucket (versioned + delete-markers handled by ListObjectVersions).
	// Both KeyMarker and VersionIdMarker must be carried forward together:
	// dropping the version marker restarts version listing from the first
	// version of the marker key on every page, skipping the rest of that
	// key's versions and leaving DeleteBucket to fail with BucketNotEmpty.
	var keyMarker, versionMarker *string
	for {
		out, err := api.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucket),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if err != nil {
			return false, err
		}
		var del []s3types.ObjectIdentifier
		for _, v := range out.Versions {
			del = append(del, s3types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			del = append(del, s3types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(del) > 0 {
			if _, err := api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3types.Delete{Objects: del},
			}); err != nil {
				return false, err
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		keyMarker = out.NextKeyMarker
		versionMarker = out.NextVersionIdMarker
	}
	if _, err := api.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return false, err
	}
	return true, nil
}

func processIAMRoleByARN(ctx context.Context, api cleanupAPI, id string, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if strings.HasPrefix(id, "role/") {
		return processIAMRole(ctx, api, strings.TrimPrefix(id, "role/"), now, ttl)
	}
	return false, nil
}

func processIAMRole(ctx context.Context, api cleanupAPI, roleName string, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	out, err := api.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
	if err != nil {
		return false, nil // role gone already
	}
	if out.Role.CreateDate == nil || !olderThanTTL(*out.Role.CreateDate, now, ttl) {
		return false, nil
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
	if _, err := api.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(roleName)}); err != nil {
		return false, err
	}
	return true, nil
}

// processRouteTable disassociates and deletes a non-main route table. The
// main route table of a VPC can't be deleted directly — it's removed
// implicitly when the VPC itself is deleted — so it's skipped here.
func processRouteTable(ctx context.Context, api cleanupAPI, id string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	out, err := api.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{id}})
	if err != nil {
		return false, err
	}
	if len(out.RouteTables) == 0 {
		return false, nil // already gone
	}
	rt := out.RouteTables[0]
	for _, assoc := range rt.Associations {
		if assoc.Main != nil && *assoc.Main {
			return false, nil
		}
	}
	for _, assoc := range rt.Associations {
		if assoc.RouteTableAssociationId == nil {
			continue
		}
		if _, err := api.DisassociateRouteTable(ctx, &ec2.DisassociateRouteTableInput{
			AssociationId: assoc.RouteTableAssociationId,
		}); err != nil {
			return false, err
		}
	}
	if _, err := api.DeleteRouteTable(ctx, &ec2.DeleteRouteTableInput{RouteTableId: aws.String(id)}); err != nil {
		return false, err
	}
	return true, nil
}

// processInternetGateway detaches an IGW from every VPC it's attached to
// before deleting it — AWS rejects DeleteInternetGateway while attached.
func processInternetGateway(ctx context.Context, api cleanupAPI, id string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	out, err := api.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{InternetGatewayIds: []string{id}})
	if err != nil {
		return false, err
	}
	if len(out.InternetGateways) == 0 {
		return false, nil // already gone
	}
	for _, att := range out.InternetGateways[0].Attachments {
		if att.VpcId == nil {
			continue
		}
		if _, err := api.DetachInternetGateway(ctx, &ec2.DetachInternetGatewayInput{
			InternetGatewayId: aws.String(id),
			VpcId:             att.VpcId,
		}); err != nil {
			return false, err
		}
	}
	if _, err := api.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{InternetGatewayId: aws.String(id)}); err != nil {
		return false, err
	}
	return true, nil
}

func processSubnet(ctx context.Context, api cleanupAPI, id string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	if _, err := api.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: aws.String(id)}); err != nil {
		return false, err
	}
	return true, nil
}

// processSecurityGroup skips the VPC's default security group: AWS refuses
// to delete it directly and it's removed implicitly with the VPC.
func processSecurityGroup(ctx context.Context, api cleanupAPI, id string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	out, err := api.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{id}})
	if err != nil {
		return false, err
	}
	if len(out.SecurityGroups) == 0 {
		return false, nil // already gone
	}
	if aws.ToString(out.SecurityGroups[0].GroupName) == "default" {
		return false, nil
	}
	if _, err := api.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: aws.String(id)}); err != nil {
		return false, err
	}
	return true, nil
}

func processVPC(ctx context.Context, api cleanupAPI, id string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	if _, err := api.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: aws.String(id)}); err != nil {
		return false, err
	}
	return true, nil
}

func processRDSSubnetGroup(ctx context.Context, api cleanupAPI, name string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	if _, err := api.DeleteDBSubnetGroup(ctx, &rds.DeleteDBSubnetGroupInput{DBSubnetGroupName: aws.String(name)}); err != nil {
		return false, err
	}
	return true, nil
}

func processRDSParameterGroup(ctx context.Context, api cleanupAPI, name string, createdAt, now time.Time, ttl time.Duration) (destroyed bool, err error) {
	if !olderThanTTL(createdAt, now, ttl) {
		return false, nil
	}
	if _, err := api.DeleteDBParameterGroup(ctx, &rds.DeleteDBParameterGroupInput{DBParameterGroupName: aws.String(name)}); err != nil {
		return false, err
	}
	return true, nil
}

// SweepReport summarises one execution of the Lambda. Destroyed/
// DestroyedCount reflect only resources the processing pass actually
// deleted; a resource whose delete call errored is counted in Errors only,
// never in Destroyed, regardless of its age.
type SweepReport struct {
	DestroyedCount int
	Errors         int
	Destroyed      []string
}

// record folds a single resource's processing outcome into the report:
// success increments DestroyedCount/Destroyed, failure logs and increments
// Errors, and a no-op (not old enough, or already gone) is silent.
func (r *SweepReport) record(kind, id string, destroyed bool, err error) {
	if err != nil {
		slog.Error("cleanup failed", "kind", kind, "id", id, "err", err)
		r.Errors++
		return
	}
	if destroyed {
		r.DestroyedCount++
		r.Destroyed = append(r.Destroyed, fmt.Sprintf("%s:%s", kind, id))
	}
}

// Sweep performs one cleanup pass. Resources are processed in dependency
// order; per-resource failures are logged but do not abort the sweep.
// SNS is only published when at least one resource was destroyed.
func Sweep(ctx context.Context, api cleanupAPI, now time.Time, ttl time.Duration, snsTopicARN string) (SweepReport, error) {
	var mappings []rgtatypes.ResourceTagMapping
	var pageToken *string
	for {
		out, err := api.GetResources(ctx, &resourcegroupstaggingapi.GetResourcesInput{
			TagFilters: []rgtatypes.TagFilter{
				{Key: aws.String("Project"), Values: []string{"redpanda-connect-bench"}},
			},
			PaginationToken: pageToken,
		})
		if err != nil {
			return SweepReport{}, fmt.Errorf("discovery: %w", err)
		}
		mappings = append(mappings, out.ResourceTagMappingList...)
		if out.PaginationToken == nil || *out.PaginationToken == "" {
			break
		}
		pageToken = out.PaginationToken
	}

	buckets := bucketByKind(mappings)
	createdAt := resourceCreatedAt(mappings)
	report := SweepReport{}

	for _, id := range buckets["rds:db"] {
		destroyed, err := processRDSInstance(ctx, api, id, now, ttl)
		report.record("rds", id, destroyed, err)
	}
	for _, id := range buckets["ec2:instance"] {
		destroyed, err := processEC2Instance(ctx, api, id, now, ttl)
		report.record("ec2", id, destroyed, err)
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
		destroyed, err := processS3Bucket(ctx, api, id, created, now, ttl)
		report.record("s3", id, destroyed, err)
	}

	for _, id := range buckets["iam:role"] {
		destroyed, err := processIAMRoleByARN(ctx, api, id, now, ttl)
		report.record("iam", id, destroyed, err)
	}

	// VPC-level chain: only reachable once instances/RDS above are gone (or
	// were already gone from a previous run). RDS subnet/parameter groups
	// go first since they logically reference the subnets deleted later in
	// this pass. Within the EC2 chain, order matters because each resource
	// depends on the next one being intact: route tables must be
	// disassociated before internet gateways are detached, subnets and
	// security groups must be gone before the VPC, and the VPC must outlive
	// all of the above.
	for _, id := range buckets["rds:subgrp"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processRDSSubnetGroup(ctx, api, id, ts, now, ttl)
		report.record("rds-subnet-group", id, destroyed, err)
	}
	for _, id := range buckets["rds:pg"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processRDSParameterGroup(ctx, api, id, ts, now, ttl)
		report.record("rds-parameter-group", id, destroyed, err)
	}
	for _, id := range buckets["ec2:route-table"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processRouteTable(ctx, api, id, ts, now, ttl)
		report.record("route-table", id, destroyed, err)
	}
	for _, id := range buckets["ec2:internet-gateway"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processInternetGateway(ctx, api, id, ts, now, ttl)
		report.record("internet-gateway", id, destroyed, err)
	}
	for _, id := range buckets["ec2:subnet"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processSubnet(ctx, api, id, ts, now, ttl)
		report.record("subnet", id, destroyed, err)
	}
	for _, id := range buckets["ec2:security-group"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processSecurityGroup(ctx, api, id, ts, now, ttl)
		report.record("security-group", id, destroyed, err)
	}
	for _, id := range buckets["ec2:vpc"] {
		ts, ok := createdAt[id]
		if !ok {
			continue
		}
		destroyed, err := processVPC(ctx, api, id, ts, now, ttl)
		report.record("vpc", id, destroyed, err)
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
