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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type createTopicConfig struct {
	srcTopic                  string
	destTopic                 string
	replicationFactorOverride bool
	replicationFactor         int
	isServerlessBroker        bool
}

func createTopic(ctx context.Context, logger *service.Logger, inputClient *kgo.Client, outputClient *kgo.Client, cfg createTopicConfig) error {
	outputAdminClient := kadm.NewClient(outputClient)

	if topics, err := outputAdminClient.ListTopics(ctx, cfg.srcTopic); err != nil {
		return fmt.Errorf("failed to fetch topic %q from output broker: %s", cfg.srcTopic, err)
	} else {
		if topics.Has(cfg.srcTopic) {
			return kerr.TopicAlreadyExists
		}
	}

	inputAdminClient := kadm.NewClient(inputClient)
	var inputTopic kadm.TopicDetail
	if topics, err := inputAdminClient.ListTopics(ctx, cfg.srcTopic); err != nil {
		return fmt.Errorf("failed to fetch topic %q from source broker: %s", cfg.srcTopic, err)
	} else {
		inputTopic = topics[cfg.srcTopic]
	}

	partitions := int32(len(inputTopic.Partitions))
	if partitions == 0 {
		partitions = -1
	}
	var rp int16
	if cfg.replicationFactorOverride {
		rp = int16(cfg.replicationFactor)
	} else {
		rp = int16(inputTopic.Partitions.NumReplicas())
		if rp == 0 {
			rp = -1
		}
	}

	topicConfigs, err := inputAdminClient.DescribeTopicConfigs(ctx, cfg.srcTopic)
	if err != nil {
		return fmt.Errorf("failed to fetch configs for topic %q from source broker: %s", cfg.srcTopic, err)
	}

	rc, err := topicConfigs.On(cfg.srcTopic, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch configs for topic %q from source broker: %s", cfg.srcTopic, err)
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
	if cfg.isServerlessBroker {
		allowedConfigs = map[string]struct{}{
			"cleanup.policy":    {},
			"retention.ms":      {},
			"max.message.bytes": {},
			"write.caching":     {},
		}
	}

	destinationConfigs := make(map[string]*string)
	for _, c := range rc.Configs {
		if _, ok := allowedConfigs[c.Key]; ok {
			destinationConfigs[c.Key] = c.Value
		}
	}

	if _, err := outputAdminClient.CreateTopic(ctx, partitions, rp, destinationConfigs, cfg.destTopic); err != nil {
		if errors.Is(err, kerr.InvalidConfig) && !cfg.isServerlessBroker {
			logger.Warnf("Invalid config detected while creating topic %q. Retrying with serverless config.", cfg.destTopic)
			cfg.isServerlessBroker = true
			return createTopic(ctx, logger, inputClient, outputClient, cfg)
		}

		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("failed to create topic %q: %s", cfg.destTopic, err)
		} else {
			return err
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
