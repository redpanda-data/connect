// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	errTopicAlreadyExists = errors.New("topic already exists")
)

func createTopic(ctx context.Context, srcTopic, destTopic string, replicationFactorOverride bool, replicationFactor int, inputClient *kgo.Client, outputClient *kgo.Client) error {
	outputAdminClient := kadm.NewClient(outputClient)

	if topics, err := outputAdminClient.ListTopics(ctx, srcTopic); err != nil {
		return fmt.Errorf("failed to fetch topic %q from output broker: %s", srcTopic, err)
	} else {
		if topics.Has(srcTopic) {
			return errTopicAlreadyExists
		}
	}

	inputAdminClient := kadm.NewClient(inputClient)
	var inputTopic kadm.TopicDetail
	if topics, err := inputAdminClient.ListTopics(ctx, srcTopic); err != nil {
		return fmt.Errorf("failed to fetch topic %q from source broker: %s", srcTopic, err)
	} else {
		inputTopic = topics[srcTopic]
	}

	partitions := int32(len(inputTopic.Partitions))
	if partitions == 0 {
		partitions = -1
	}
	var rp int16
	if replicationFactorOverride {
		rp = int16(replicationFactor)
	} else {
		rp = int16(inputTopic.Partitions.NumReplicas())
		if rp == 0 {
			rp = -1
		}
	}

	topicConfigs, err := inputAdminClient.DescribeTopicConfigs(ctx, srcTopic)
	if err != nil {
		return fmt.Errorf("failed to fetch configs for topic %q from source broker: %s", srcTopic, err)
	}

	rc, err := topicConfigs.On(srcTopic, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch configs for topic %q from source broker: %s", srcTopic, err)
	}

	// Source: https://docs.redpanda.com/current/reference/properties/topic-properties/
	allowedConfigs := map[string]struct{}{
		"cleanup.policy":                    {},
		"flush.bytes":                       {},
		"flush.ms":                          {},
		"initial.retention.local.target.ms": {},
		"retention.bytes":                   {},
		"retention.ms":                      {},
		"segment.ms":                        {},
		"segment.bytes":                     {},
		"compression.type":                  {},
		"message.timestamp.type":            {},
		"max.message.bytes":                 {},
		"replication.factor":                {},
		"write.caching":                     {},
		"redpanda.iceberg.mode":             {},
	}

	destinationConfigs := make(map[string]*string)
	for _, c := range rc.Configs {
		if _, ok := allowedConfigs[c.Key]; ok {
			destinationConfigs[c.Key] = c.Value
		}
	}

	if _, err := outputAdminClient.CreateTopic(ctx, partitions, rp, destinationConfigs, destTopic); err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("failed to create topic %q: %s", destTopic, err)
		}
	}

	return nil
}

func createACLs(ctx context.Context, srcTopic, destTopic string, inputClient *kgo.Client, outputClient *kgo.Client) error {
	inputAdminClient := kadm.NewClient(inputClient)
	outputAdminClient := kadm.NewClient(outputClient)

	// Only topic ACLs are migrated, group ACLs are not migrated.
	// Users are not migrated because we can't read passwords.

	builder := kadm.NewACLs().Topics(srcTopic).
		ResourcePatternType(kadm.ACLPatternLiteral).Operations().Allow().Deny().AllowHosts().DenyHosts()
	var inputACLResults kadm.DescribeACLsResults
	var err error
	if inputACLResults, err = inputAdminClient.DescribeACLs(ctx, builder); err != nil {
		return fmt.Errorf("failed to fetch ACLs for topic %q: %s", srcTopic, err)
	}

	if len(inputACLResults) > 1 {
		return fmt.Errorf("received unexpected number of ACL results for topic %q: %d", srcTopic, len(inputACLResults))
	}

	for _, acl := range inputACLResults[0].Described {
		builder := kadm.NewACLs()

		if acl.Permission == kmsg.ACLPermissionTypeAllow && acl.Operation == kmsg.ACLOperationWrite {
			// ALLOW WRITE ACLs for topics are not migrated.
			continue
		}

		op := acl.Operation
		if op == kmsg.ACLOperationAll {
			// ALLOW ALL ACLs for topics are downgraded to ALLOW READ.
			op = kmsg.ACLOperationRead
		}
		switch acl.Permission {
		case kmsg.ACLPermissionTypeAllow:
			builder = builder.Allow(acl.Principal).AllowHosts(acl.Host).Topics(destTopic).ResourcePatternType(acl.Pattern).Operations(op)
		case kmsg.ACLPermissionTypeDeny:
			builder = builder.Deny(acl.Principal).DenyHosts(acl.Host).Topics(destTopic).ResourcePatternType(acl.Pattern).Operations(op)
		}

		// Attempting to overwrite existing ACLs is idempotent and doesn't seem to raise an error.
		if _, err := outputAdminClient.CreateACLs(ctx, builder); err != nil {
			return fmt.Errorf("failed to create ACLs for topic %q: %s", destTopic, err)
		}
	}

	return nil
}
