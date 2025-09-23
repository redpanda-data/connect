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
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TopicMigratorConfig controls how topics are created and synchronised on the
// destination cluster during migration.
type TopicMigratorConfig struct {
	// NameResolver is an optional template used to derive the destination topic
	// name from a source topic. When nil, the source name is used as-is.
	NameResolver *service.InterpolatedString
	// RF is the replication factor for new topics. Zero means inherit from the
	// source topic.
	RF int
	// SyncACLs enables copying ACLs from the source topic to the destination
	// topic, applying basic transformations where necessary.
	SyncACLs bool
	// Serverless narrows the set of topic configuration keys to those supported
	// by serverless clusters.
	Serverless bool
}

func (m *TopicMigratorConfig) initFromParsed(pConf *service.ParsedConfig) error {
	var err error

	if pConf.Contains(rmoFieldTopic) {
		if m.NameResolver, err = pConf.FieldInterpolatedString(rmoFieldTopic); err != nil {
			return fmt.Errorf("get topic field: %w", err)
		}
	}

	if pConf.Contains(rmoFieldTopicReplicationFactor) {
		if m.RF, err = pConf.FieldInt(rmoFieldTopicReplicationFactor); err != nil {
			return fmt.Errorf("get topic replication factor field: %w", err)
		}
	}

	m.SyncACLs, err = pConf.FieldBool(rmoFieldSyncTopicACLs)
	if err != nil {
		return fmt.Errorf("get sync topic ACLs field: %w", err)
	}

	m.Serverless, err = pConf.FieldBool(rmoFieldServerless)
	if err != nil {
		return fmt.Errorf("get serverless field: %w", err)
	}

	return nil
}

func (m *TopicMigratorConfig) supportedTopicConfigs() []string {
	if m.Serverless {
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

// TopicInfo describes a topic by name and partition count as observed on a
// cluster. Partitions is the number of partitions currently reported.
type TopicInfo struct {
	Topic      string
	Partitions int
}

// TopicMapping pairs a source topic with its resolved destination topic,
// including their names and partition counts.
type TopicMapping struct {
	Src TopicInfo
	Dst TopicInfo
}

// topicMigrator coordinates topic migration between clusters.
//
// Responsibilities:
//   - Resolve destination topic names from source names.
//   - Create destination topics mirroring partitions and selected replication factor.
//   - Copy supported topic configurations (serverless-aware subset).
//   - Optionally synchronise ACLs.
//   - Cache known topics to avoid redundant work.
type topicMigrator struct {
	conf    TopicMigratorConfig
	metrics *topicMetrics
	log     *service.Logger

	mu          sync.RWMutex
	knownTopics map[string]TopicMapping // source topic name -> source and destination topic info
}

// SyncOnce runs the topic sync once if the set of known topics is empty, and
// does nothing otherwise.
func (m *topicMigrator) SyncOnce(
	ctx context.Context,
	srcAdm, dstAdm *kadm.Client,
	topics func() []string,
) error {
	if m.hasKnownTopics() {
		return nil
	}
	m.log.Infof("Topic migration: starting initial topic sync")
	return m.Sync(ctx, srcAdm, dstAdm, topics)
}

// hasKnownTopics returns true if there are any known topics.
func (m *topicMigrator) hasKnownTopics() bool {
	m.mu.RLock()
	n := len(m.knownTopics)
	m.mu.RUnlock()

	return n > 0
}

// Sync ensures that all topics returned by the given function exist in the
// destination cluster, with mirroring partition counts and a selected
// replication factor. If the topics function returns zero topics, this
// function does nothing. It also remembers the created topics to avoid
// redundant lookups and creations.
func (m *topicMigrator) Sync(
	ctx context.Context,
	srcAdm, dstAdm *kadm.Client,
	getTopics func() []string,
) error {
	all := getTopics()

	if len(all) == 0 {
		m.log.Debugf("Topic migration: no topics to sync")
		return nil
	}

	m.log.Infof("Topic migration: syncing %d topics", len(all))

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range all {
		if t == "" {
			m.log.Debugf("Topic migration: skip empty topic name")
			continue
		}
		if _, ok := m.knownTopics[t]; ok {
			m.log.Debugf("Topic migration: topic '%s' already known, skipping creation", t)
			continue
		}

		if err := m.createTopicLocked(ctx, srcAdm, dstAdm, t); err != nil {
			return fmt.Errorf("create topic %s: %w", t, err)
		}
	}

	return nil
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

	if err := m.createTopicLocked(ctx, srcAdm, dstAdm, topic); err != nil {
		return "", err
	}

	return m.knownTopics[topic].Dst.Topic, nil
}

func (m *topicMigrator) createTopicLocked(ctx context.Context, srcAdm, dstAdm *kadm.Client, topic string) error {
	if _, ok := m.cachedTopicLocked(topic); ok {
		return nil
	}

	m.log.Debugf("Topic migration: creating topic '%s'", topic)

	dstTopic, err := m.resolveTopic(topic)
	if err != nil {
		return err
	}
	m.log.Debugf("Topic migration: resolved '%s' to destination topic '%s'", topic, dstTopic)

	info, rc, err := topicDetailsWithClient(ctx, srcAdm, topic)
	if err != nil {
		return fmt.Errorf("get topic details %s: %w", topic, err)
	}
	partitions := int32(len(info.Partitions))
	if partitions == 0 {
		partitions = -1
	}
	m.log.Debugf("Topic migration: partition count for '%s': %d", topic, partitions)

	var rf int16
	if m.conf.Serverless {
		rf = -1
	} else {
		rf = m.topicReplicationFactor(info.Partitions.NumReplicas())
	}
	m.log.Debugf("Topic migration: replication factor for '%s': %d", topic, rf)

	conf := newTopicConfig(rc.Configs, m.conf.supportedTopicConfigs())
	m.log.Debugf("Topic migration: configuration for '%s':\n%s", topic, conf)

	tm := TopicMapping{
		Src: TopicInfo{
			Topic:      topic,
			Partitions: len(info.Partitions),
		},
		Dst: TopicInfo{
			Topic:      dstTopic,
			Partitions: len(info.Partitions),
		},
	}

	t0 := time.Now()
	_, err = dstAdm.CreateTopic(ctx, partitions, rf, conf, dstTopic)
	if err != nil && errors.Is(err, kerr.TopicAlreadyExists) {
		m.log.Infof("Topic migration: destination topic '%s' for source '%s' already exists", dstTopic, topic)

		dstInfo, _, err := topicDetailsWithClient(ctx, srcAdm, dstTopic)
		if err != nil {
			return fmt.Errorf("get destination topic details %s: %w", dstTopic, err)
		}
		if len(dstInfo.Partitions) != len(info.Partitions) {
			m.log.Warnf("Topic migration: topic partitions mismatch: got %d expected %d - this would disable conumer group migration for this topic", len(dstInfo.Partitions), len(info.Partitions))
			tm.Dst.Partitions = len(dstInfo.Partitions)
		}
	} else if err != nil {
		m.metrics.IncCreateErrors()
		return fmt.Errorf("create topic %q: %w", topic, err)
	} else {
		m.metrics.ObserveCreateLatency(time.Since(t0))
		m.metrics.IncCreated()
		m.log.Infof("Topic migration: successfully created destination topic '%s' for source '%s'", dstTopic, topic)
	}

	if syncErr := m.SyncACLs(ctx, srcAdm, dstAdm, topic, dstTopic); syncErr != nil {
		return fmt.Errorf("sync ACLs for topic %s: %w", dstTopic, syncErr)
	}

	m.knownTopics[topic] = tm
	return nil
}

func (m *topicMigrator) cachedTopic(topic string) (dstTopic string, ok bool) {
	m.mu.RLock()
	dstTopic, ok = m.cachedTopicLocked(topic)
	m.mu.RUnlock()
	return
}

func (m *topicMigrator) cachedTopicLocked(topic string) (dstTopic string, ok bool) {
	v, ok := m.knownTopics[topic]
	return v.Dst.Topic, ok
}

func (m *topicMigrator) resolveTopic(topic string) (string, error) {
	if m.conf.NameResolver == nil {
		return topic, nil
	}

	// Hack: The current message corresponds to a specific topic, but we want to
	// create all topics, so we assume users will only use the `kafka_topic`
	// metadata when specifying the `topic`.
	msg := service.NewMessage(nil)
	msg.MetaSetMut("kafka_topic", topic)

	dstTopic, err := m.conf.NameResolver.TryString(msg)
	if err != nil {
		return "", fmt.Errorf("resolve destination topic: %s", err)
	}
	if dstTopic == "" {
		return "", errors.New("resolved empty destination topic")
	}
	return dstTopic, nil
}

func (m *topicMigrator) topicReplicationFactor(rf int) int16 {
	if m.conf.RF != 0 {
		return int16(m.conf.RF)
	}

	return int16(rf)
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
	if !m.conf.SyncACLs {
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

		results, err := dstAdm.CreateACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("create ACLs for topic %s: %w", dstTopic, err)
		}
		for _, r := range results {
			if err := r.Err; err != nil {
				return fmt.Errorf("create ACLs for topic %s: %w: %s", dstTopic, err, r.ErrMessage)
			}
			m.log.Debugf("Topic migration: created ACL %v", r)
		}
	}

	m.log.Infof("Topic migration: successfully synchronised ACLs from source '%s' to destination '%s'",
		srcTopic, dstTopic)

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
			return nil, fmt.Errorf("describe ACLs for topic %q: %w: %s", topic, res.Err, res.ErrMessage)
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

// TopicMapping returns a slice of known topic mappings, sorted by source topic name.
// The slice is read-only and valid until the next call to `Sync` or `SyncOnce`.
// Each TopicMapping describes a topic by name and partition count as observed on a
// cluster. Partitions is the number of partitions currently reported.
func (m *topicMigrator) TopicMapping() []TopicMapping {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s := make([]TopicMapping, 0, len(m.knownTopics))
	for _, tm := range m.knownTopics {
		s = append(s, tm)
	}
	slices.SortFunc(s, func(a, b TopicMapping) int {
		return strings.Compare(a.Src.Topic, b.Src.Topic)
	})

	return s
}

type topicMetrics struct {
	created       *service.MetricCounter
	createErrors  *service.MetricCounter
	createLatency *service.MetricTimer
}

func newTopicMetrics(m *service.Metrics) *topicMetrics {
	return &topicMetrics{
		created:       m.NewCounter("migrator_topics_created_total"),
		createErrors:  m.NewCounter("migrator_topic_create_errors_total"),
		createLatency: m.NewTimer("migrator_topic_create_latency_ns"),
	}
}

func (tm *topicMetrics) IncCreated() {
	if tm == nil {
		return
	}
	tm.created.Incr(1)
}

func (tm *topicMetrics) IncCreateErrors() {
	if tm == nil {
		return
	}
	tm.createErrors.Incr(1)
}

func (tm *topicMetrics) ObserveCreateLatency(d time.Duration) {
	if tm == nil {
		return
	}
	tm.createLatency.Timing(d.Nanoseconds())
}
