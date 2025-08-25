// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// topicMigrator is responsible for resolving topic names, migrating topics
// including the topic replication factor and configuration.
type topicMigrator struct {
	log *service.Logger

	nameResolver *service.InterpolatedString
	rf           int
	syncACLs     bool
	serverless   bool

	mu          sync.RWMutex
	knownTopics map[string]string // source topic -> destination topic
}

func (m *topicMigrator) initFromParsed(pConf *service.ParsedConfig) error {
	var err error

	if pConf.Contains(rmoFieldTopic) {
		if m.nameResolver, err = pConf.FieldInterpolatedString(rmoFieldTopic); err != nil {
			return fmt.Errorf("get topic field: %w", err)
		}
	}

	if pConf.Contains(rmoFieldTopicReplicationFactor) {
		if m.rf, err = pConf.FieldInt(rmoFieldTopicReplicationFactor); err != nil {
			return fmt.Errorf("get topic replication factor field: %w", err)
		}
	}

	m.syncACLs, err = pConf.FieldBool(rmoFieldSyncTopicACLs)
	if err != nil {
		return fmt.Errorf("get sync topic ACLs field: %w", err)
	}

	m.serverless, err = pConf.FieldBool(rmoFieldIsServerless)
	if err != nil {
		return fmt.Errorf("get serverless field: %w", err)
	}

	return nil
}

// InitKnownTopics creates all known topics provided if knownTopics is empty.
func (m *topicMigrator) InitKnownTopics(
	ctx context.Context,
	srcAdm, dstAdm *kadm.Client,
	topics func() []string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.knownTopics) > 0 {
		return nil
	}

	var merr []error
	for _, topic := range topics() {
		dstTopic, err := m.createTopicLocked(ctx, srcAdm, dstAdm, topic)
		if err != nil {
			merr = append(merr, fmt.Errorf("create topic %s: %w", topic, err))
		}
		m.knownTopics[topic] = dstTopic
	}

	return errors.Join(merr...)
}

// CreateTopicIfNeeded creates the topic if it does not already exist.
func (m *topicMigrator) CreateTopicIfNeeded(
	ctx context.Context,
	srcAdm, dstAdm *kadm.Client,
	topic string,
) (string, error) {
	if topic == "" {
		return "", errors.New("topic name cannot be empty")
	}

	if dstTopic, ok := m.cachedTopic(topic); ok {
		return dstTopic, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.createTopicLocked(ctx, srcAdm, dstAdm, topic)
}

func (m *topicMigrator) createTopicLocked(ctx context.Context, srcAdm, dstAdm *kadm.Client, topic string) (string, error) {
	if dstTopic, ok := m.cachedTopicLocked(topic); ok {
		return dstTopic, nil
	}

	m.log.Debugf("Topic migration: creating topic '%s'", topic)

	dstTopic, err := m.resolveTopic(topic)
	if err != nil {
		return "", err
	}
	m.log.Debugf("Topic migration: resolved '%s' to destination topic '%s'", topic, dstTopic)

	info, rc, err := topicDetailsWithClient(ctx, srcAdm, topic)
	if err != nil {
		return "", fmt.Errorf("get topic details %s: %w", topic, err)
	}
	partitions := int32(len(info.Partitions))
	if partitions == 0 {
		partitions = -1
	}
	rf := m.topicReplicationFactor(info.Partitions.NumReplicas())
	m.log.Debugf("Topic migration: source '%s' has %d partitions with replication factor %d", topic, partitions, rf)

	conf := newTopicConfig(rc.Configs, m.supportedTopicConfigs())
	m.log.Debugf("Topic migration: configuration for '%s':\n%s", topic, conf)

	if _, err := dstAdm.CreateTopic(ctx, partitions, rf, conf, dstTopic); err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			err = nil
		}
		return "", fmt.Errorf("create topic %q: %w", topic, err)
	}
	m.log.Infof("Topic migration: successfully created destination topic '%s' for source '%s'", dstTopic, topic)

	if m.SyncACLs(ctx, srcAdm, dstAdm, topic, dstTopic) != nil {
		return "", fmt.Errorf("sync ACLs for topic %s: %w", dstTopic, err)
	}

	m.knownTopics[topic] = dstTopic
	return dstTopic, nil
}

func (m *topicMigrator) cachedTopic(topic string) (dstTopic string, ok bool) {
	m.mu.RLock()
	dstTopic, ok = m.cachedTopicLocked(topic)
	m.mu.RUnlock()
	return
}

func (m *topicMigrator) cachedTopicLocked(topic string) (dstTopic string, ok bool) {
	dstTopic, ok = m.knownTopics[topic]
	return
}

func (m *topicMigrator) resolveTopic(topic string) (string, error) {
	if m.nameResolver == nil {
		return topic, nil
	}

	// Hack: The current message corresponds to a specific topic, but we want to
	// create all topics, so we assume users will only use the `kafka_topic`
	// metadata when specifying the `topic`.
	msg := service.NewMessage(nil)
	msg.MetaSetMut("kafka_topic", topic)

	dstTopic, err := m.nameResolver.TryString(msg)
	if err != nil {
		return "", fmt.Errorf("resolve destination topic: %s", err)
	}
	if dstTopic == "" {
		return "", errors.New("resolved empty destination topic")
	}
	return dstTopic, nil
}

func (m *topicMigrator) topicReplicationFactor(rf int) int16 {
	if m.rf != 0 {
		return int16(m.rf)
	}

	return int16(rf)
}

func (m *topicMigrator) supportedTopicConfigs() []string {
	if m.serverless {
		return []string{
			"cleanup.policy",
			"retention.ms",
			"max.message.bytes",
			"write.caching",
		}
	}

	// Source: https://docs.redpanda.com/current/reference/properties/topic-properties/
	return []string{
		"cleanup.policy",
		"flush.bytes",
		"flush.ms",
		"initial.retention.local.target.ms",
		"retention.bytes",
		"retention.ms",
		"segment.ms",
		"segment.bytes",
		"compression.type",
		"message.timestamp.type",
		"max.message.bytes",
	}
}

func topicDetailsWithClient(ctx context.Context, adm *kadm.Client, topic string) (kadm.TopicDetail, kadm.ResourceConfig, error) {
	var (
		d  kadm.TopicDetail
		rc kadm.ResourceConfig
	)

	{
		topics, err := adm.ListTopics(ctx, topic)
		if err != nil {
			return d, rc, err
		}

		var ok bool
		d, ok = topics[topic]
		if !ok {
			return d, rc, fmt.Errorf("topic %s not found", topic)
		}

		if d.Err != nil {
			return d, rc, d.Err
		}
	}

	{
		rcs, err := adm.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			return d, rc, err
		}
		rc, err = rcs.On(topic, nil)
		if err != nil {
			return d, rc, err
		}
		if rc.Err != nil {
			return d, rc, rc.Err
		}
	}

	return d, rc, nil
}

type topicConfig map[string]*string

func newTopicConfig(configs []kadm.Config, supported []string) topicConfig {
	tc := make(map[string]*string, len(supported))
	for _, c := range configs {
		if slices.Contains(supported, c.Key) {
			tc[c.Key] = c.Value
		}
	}
	return tc
}

func (c topicConfig) String() string {
	var buf []byte
	for k, v := range c {
		var sv string
		if v != nil {
			sv = *v
		}
		buf = fmt.Appendf(buf, "%s=%s\n", k, sv)
	}
	return string(buf)
}

// SyncACLs copies ACLs from source topic to destination topic.
func (m *topicMigrator) SyncACLs(
	ctx context.Context,
	srcAdm, dstAdm *kadm.Client,
	srcTopic, dstTopic string,
) error {
	if !m.syncACLs {
		return nil
	}

	m.log.Debugf("Topic migration: synchronising ACLs from '%s' to '%s'", srcTopic, dstTopic)

	described, err := describeACLs(ctx, srcAdm, srcTopic)
	if err != nil {
		return fmt.Errorf("describe ACLs for topic %s: %w", srcTopic, err)
	}
	if len(described) == 0 {
		m.log.Debugf("Topic migration: no ACLs found for source topic '%s'", srcTopic)
		return nil
	}

	for _, acl := range described {
		// Filter ACLs that shouldn't be replicated
		if !shouldReplicateACL(acl) {
			m.log.Debugf("Topic migration: skipping ACL from '%s' to '%s' for principal '%v' with permission '%v' and operation '%v'",
				srcTopic, dstTopic, acl.Principal, acl.Permission, acl.Operation)
			continue
		}

		b := aclBuilderFromDescribed(dstTopic, transformACLForTarget(acl))
		if b == nil {
			continue
		}
		if _, err := dstAdm.CreateACLs(ctx, b); err != nil {
			return fmt.Errorf("create ACLs for topic %s: %w", dstTopic, err)
		}
	}

	m.log.Infof("Topic migration: successfully synchronised ACLs from source '%s' to destination '%s'", srcTopic, dstTopic)

	return nil
}

// shouldReplicateACL implements logic similar to shouldReplicateAcl in MM2.
// See: https://github.com/apache/kafka/blob/25da7051785b35e7097ee41b430f212e7eafb2f4/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConnector.java#L703
func shouldReplicateACL(acl kadm.DescribedACL) bool {
	// Don't replicate ALLOW WRITE operations
	return !(acl.Permission == kmsg.ACLPermissionTypeAllow && acl.Operation == kmsg.ACLOperationWrite) //nolint:staticcheck // comprehension
}

// transformACLForTarget implement logic similar to targetAclBinding in MM2.
// See: https://github.com/apache/kafka/blob/25da7051785b35e7097ee41b430f212e7eafb2f4/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceConnector.java#L685
func transformACLForTarget(acl kadm.DescribedACL) kadm.DescribedACL {
	// If this is an ALLOW ALL operation, downgrade to READ
	if acl.Permission == kmsg.ACLPermissionTypeAllow &&
		acl.Operation == kmsg.ACLOperationAll {
		acl.Operation = kmsg.ACLOperationRead
	}
	return acl
}

func describeACLs(ctx context.Context, srcAdm *kadm.Client, topic string) ([]kadm.DescribedACL, error) {
	b := kadm.NewACLs().
		Topics(topic).
		ResourcePatternType(kadm.ACLPatternLiteral). // Exact match - default
		Operations(kmsg.ACLOperationAny).            // Any operation - default
		Allow().AllowHosts().                        // Allow any
		Deny().DenyHosts()                           // Deny any
	results, err := srcAdm.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("describe ACLs for topic %q: %w", topic, err)
	}

	var all []kadm.DescribedACL
	for _, res := range results {
		if res.Err != nil {
			return nil, fmt.Errorf("describe ACLs for topic %q: %w", topic, res.Err)
		}
		all = append(all, res.Described...)
	}

	return all, nil
}

func aclBuilderFromDescribed(topic string, acl kadm.DescribedACL) *kadm.ACLBuilder {
	b := kadm.NewACLs().
		Topics(topic).
		Operations(acl.Operation).
		ResourcePatternType(acl.Pattern)

	switch acl.Permission {
	case kmsg.ACLPermissionTypeAllow:
		if acl.Host == "" {
			b.Allow(acl.Principal)
		} else {
			b.Allow(acl.Principal).AllowHosts(acl.Host)
		}
	case kmsg.ACLPermissionTypeDeny:
		if acl.Host == "" {
			b.Deny(acl.Principal)
		} else {
			b.Deny(acl.Principal).DenyHosts(acl.Host)
		}
	default:
		return nil // should never happen but we only support allow/deny
	}

	return b
}
