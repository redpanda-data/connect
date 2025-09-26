// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"
)

const (
	groupsObjectField = "consumer_groups"

	cgFieldEnabled   = "enabled"
	cgFieldInterval  = "interval"
	cgFieldFetchTime = "fetch_timeout"
	cgFieldInclude   = "include"
	cgFieldExclude   = "exclude"
	cgFieldOnlyEmpty = "only_empty"
)

// GroupsMigratorConfig controls consumer groups migration scope.
type GroupsMigratorConfig struct {
	// Enabled toggles consumer groups migration.
	Enabled bool
	// Interval controls how often to synchronise consumer groups. Zero means one-shot.
	Interval time.Duration
	// FetchTimeout is the maximum time to wait for data when fetching records for timestamp translation.
	FetchTimeout time.Duration
	confx.RegexpFilter
	// OnlyEmpty controls which consumer group states to include in migration.
	// When false (default), all statuses except Dead are included.
	// When true, only Empty groups are considered.
	OnlyEmpty bool
	// SkipSourceGroup when set prevents the migrator from attempting to migrate
	// its own consumer group.
	SkipSourceGroup string
}

// groupsMigratorFields returns the config fields for consumer groups migrator.
func groupsMigratorFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBoolField(cgFieldEnabled).
			Description("Whether consumer group offset migration is enabled. When disabled, no consumer group operations are performed.").
			Default(true),
		service.NewDurationField(cgFieldInterval).
			Description("How often to synchronise consumer group offsets. Regular syncing helps maintain offset accuracy during ongoing migration.").
			Example("0s     # Disabled").
			Example("30s    # Sync every 30 seconds").
			Example("5m     # Sync every 5 minutes").
			Default("1m"),
		service.NewDurationField(cgFieldFetchTime).
			Description("Maximum time to wait for data when fetching records for timestamp-based offset translation. Increase for clusters with low message throughput.").
			Example("1s     # Fast clusters").
			Example("10s    # Slower clusters").
			Default("10s"),
		service.NewStringListField(cgFieldInclude).
			Description("Regular expressions for consumer groups to include in offset migration. If empty, all groups are included (unless excluded).").
			Example(`["prod-.*", "staging-.*"]`).
			Example(`["app-.*", "service-.*"]`).
			Optional(),
		service.NewStringListField(cgFieldExclude).
			Description("Regular expressions for consumer groups to exclude from offset migration. Takes precedence over include patterns. Useful for excluding system or temporary groups.").
			Example(`[".*-test", ".*-temp", "connect-.*"]`).
			Example(`["dev-.*", "local-.*"]`).
			Optional(),
		service.NewBoolField(cgFieldOnlyEmpty).
			Description("Whether to only migrate Empty consumer groups. When false (default), all statuses except Dead are included; when true, only Empty groups are migrated.").
			Default(false),
	}
}

// initFromParsed initializes the groups migrator config from parsed config.
func (c *GroupsMigratorConfig) initFromParsed(pConf *service.ParsedConfig) error {
	if !pConf.Contains(groupsObjectField) {
		return nil
	}
	pConf = pConf.Namespace(groupsObjectField)

	var err error

	// Enabled flag
	if c.Enabled, err = pConf.FieldBool(cgFieldEnabled); err != nil {
		return fmt.Errorf("parse enabled setting: %w", err)
	}

	// Interval setting
	if c.Interval, err = pConf.FieldDuration(cgFieldInterval); err != nil {
		return fmt.Errorf("parse interval setting: %w", err)
	}

	// FetchTimeout setting
	if c.FetchTimeout, err = pConf.FieldDuration(cgFieldFetchTime); err != nil {
		return fmt.Errorf("parse fetch_timeout setting: %w", err)
	}

	// Include regex patterns
	if pConf.Contains(cgFieldInclude) {
		patterns, err := pConf.FieldStringList(cgFieldInclude)
		if err != nil {
			return fmt.Errorf("parse include patterns: %w", err)
		}
		c.Include, err = confx.ParseRegexpPatterns(patterns)
		if err != nil {
			return fmt.Errorf("invalid include regex patterns: %w", err)
		}
	}

	// Exclude regex patterns
	if pConf.Contains(cgFieldExclude) {
		patterns, err := pConf.FieldStringList(cgFieldExclude)
		if err != nil {
			return fmt.Errorf("parse exclude patterns: %w", err)
		}
		c.Exclude, err = confx.ParseRegexpPatterns(patterns)
		if err != nil {
			return fmt.Errorf("invalid exclude regex patterns: %w", err)
		}
	}

	// OnlyEmpty setting
	if c.OnlyEmpty, err = pConf.FieldBool(cgFieldOnlyEmpty); err != nil {
		return fmt.Errorf("parse only_empty setting: %w", err)
	}

	return nil
}

// initFromParsedInput initializes the groups migrator config from input config.
// This reads the consumer group from the input configuration and sets it as
// the source group to skip during migration.
func (c *GroupsMigratorConfig) initFromParsedInput(pConf *service.ParsedConfig) error {
	if pConf == nil {
		return nil
	}

	var err error

	c.SkipSourceGroup, err = pConf.FieldString("consumer_group")
	if err != nil {
		return fmt.Errorf("parse consumer_group from input: %w", err)
	}

	return nil
}

// GroupOffset is a tuple of group name and offset (topic, partition, position).
type GroupOffset struct {
	Group string
	kadm.Offset
}

// groupsMigrator migrates consumer group offsets between Kafka/Redpanda clusters.
//
// It synchronises consumer group positions from source to destination cluster
// using timestamp-based offset translation. By default it migrates consumer
// groups in all states except "Dead". When `only_empty` is true, it only
// includes groups in "Empty" state.
//
// Responsibilities:
//   - Discovers and filters consumer groups by name patterns and state
//   - Translates offsets using record timestamps between clusters
//   - Commits translated offsets while preventing position rewinding
//   - Runs in one-shot or continuous sync modes
//   - Provides metrics and caching for performance
type groupsMigrator struct {
	conf    GroupsMigratorConfig
	src     *kgo.Client
	srcAdm  *kadm.Client
	dstAdm  *kadm.Client
	metrics *groupsMetrics
	log     *service.Logger

	topicIDs map[string]kadm.TopicID
	// commitedOffsets is a map of group -> topic -> partition -> (src.offset, dst.offset)
	// it's used to avoid committing the same offset twice.
	commitedOffsets map[string]map[string]map[int32][2]int64
}

// ListGroupOffsets returns a list of committed offsets for all consumer groups
// in the source cluster filtered by the given topics.
//
// The method applies multiple filtering rules to determine which consumer groups
// and their offsets are returned:
//
//  1. Consumer Group Name Filtering: Groups are filtered using regex patterns
//     configured via include/exclude settings. Only groups matching the include
//     pattern (if set) and not matching the exclude pattern (if set) are kept.
//
//  2. Group State Filtering: By default (only_empty=false) consumer groups
//     in all states except "Dead" are included. When only_empty=true,
//     only groups in "Empty" state are included.
//
//  3. Topic-Based Offset Filtering: Groups are removed if they have no committed
//     offsets for any of the specified topics. A group is only kept if it has at
//     least one committed offset for at least one of the requested topics.
//
// The returned GroupOffset slice contains all committed offsets for the filtered
// groups, sorted by group name for consistent ordering.
func (m *groupsMigrator) ListGroupOffsets(ctx context.Context, topics []string) ([]GroupOffset, error) {
	if m.srcAdm == nil {
		return nil, errors.New("source admin client not configured")
	}
	return m.listGroupsOffsets(ctx, m.srcAdm, topics)
}

func (m *groupsMigrator) listGroupsOffsets(ctx context.Context, adm *kadm.Client, topics []string) ([]GroupOffset, error) {
	// List groups
	cg, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	groups := m.conf.Filtered(cg.Groups())

	// Filter out active groups, possible values are:
	// * Dead – the group has no members and no active metadata; effectively removed.
	// * Empty – no active members, but group metadata (like offsets) still exists.
	// * PreparingRebalance – group is in the process of rebalancing, waiting for members to rejoin.
	// * CompletingRebalance – all members have joined, and assignments are being finalized.
	// * Stable – group has members, assignments are completed, and it is operating normally.
	// See: https://kafka.apache.org/40/javadoc/org/apache/kafka/common/GroupState.html
	groups = slices.DeleteFunc(groups, func(g string) bool {
		st := cg[g].State
		var allowed bool
		if m.conf.OnlyEmpty {
			allowed = st == "Empty"
		} else {
			allowed = st != "Dead"
		}
		if !allowed {
			m.log.Debugf("Consumer group migration: skipping group '%s' with state '%s'", g, st)
		}
		return !allowed
	})

	// Filter out groups with no offsets for any topic we're interested in
	resp := m.srcAdm.FetchManyOffsets(ctx, groups...)
	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	groups = slices.DeleteFunc(groups, func(g string) bool {
		for _, t := range topics {
			if len(resp[g].Fetched[t]) > 0 {
				return false
			}
		}
		m.log.Debugf("Consumer group migration: skipping group '%s' with no offsets for any topic", g)
		return true
	})

	// Sort and convert to group offsets
	sort.Strings(groups)

	gcos := make([]GroupOffset, 0, len(groups))
	for _, g := range groups {
		for _, p := range resp[g].Fetched {
			for _, o := range p {
				gcos = append(gcos, GroupOffset{
					Group:  g,
					Offset: o.Offset,
				})
			}
		}
	}

	return gcos, nil
}

// SyncLoop runs the consumer groups sync in a loop at the configured interval
// until ctx is done. If interval is <= 0, the loop is not started.
func (m *groupsMigrator) SyncLoop(ctx context.Context, getTopics func() []TopicMapping) {
	if !m.enabled() {
		m.log.Info("Consumer group migration: consumer group sync disabled")
		return
	}
	if m.conf.Interval <= 0 {
		m.log.Info("Consumer group migration: consumer group sync disabled (interval <= 0)")
		return
	}

	m.log.Infof("Consumer group migration: starting consumer group sync loop every %s", m.conf.Interval)

	t := time.NewTicker(m.conf.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Consumer group migration: stopping consumer group sync loop")
			return
		case <-t.C:
			if err := m.Sync(ctx, getTopics); err != nil {
				m.log.Errorf("Consumer group migration: sync error: %v", err)
			}
		}
	}
}

// Sync syncs consumer groups offsets between two Redpanda/Kafka clusters.
func (m *groupsMigrator) Sync(ctx context.Context, getTopics func() []TopicMapping) error {
	if !m.enabled() {
		m.log.Info("Consumer group migration: consumer group sync disabled")
		return nil
	}

	m.log.Info("Consumer group migration: syncing consumer groups")

	mappings := getTopics()

	// Filter out topics
	topics := m.filterTopics(mappings)
	if len(topics) == 0 {
		m.log.Infof("Consumer group migration: no topics to sync")
		return nil
	}

	// List group offsets, and remove already synced groups
	gcos, err := m.ListGroupOffsets(ctx, topics)
	if err != nil {
		return err
	}
	// Initialize committed offsets cache and filter out already synced groups
	gcos = slices.DeleteFunc(gcos, func(gco GroupOffset) bool {
		g := gco.Group
		t := gco.Topic
		p := gco.Partition

		if g == m.conf.SkipSourceGroup {
			return true
		}

		if m.commitedOffsets[g] == nil {
			m.commitedOffsets[g] = make(map[string]map[int32][2]int64)
		}
		if m.commitedOffsets[g][t] == nil {
			m.commitedOffsets[g][t] = make(map[int32][2]int64)
		}

		// Already synced
		if co := m.commitedOffsets[g][t][p]; co[0] >= gco.At && co[1] != 0 {
			m.log.Debugf("Consumer group migration: group '%s' topic '%s' partition '%d' already synced - skipping", g, t, p)
			return true
		}

		// Mark as not synced
		m.commitedOffsets[g][t][p] = [2]int64{gco.At, 0}

		return false
	})
	if len(gcos) == 0 {
		m.log.Infof("Consumer group migration: nothing to do")
		return nil
	}
	topics = extractTopics(gcos)

	m.log.Debugf("Consumer group migration: syncing groups %s", extractGroupNames(gcos))

	if err := m.fillTopicIDs(ctx, topics); err != nil {
		return err
	}

	// List start and end offsets for topics
	tso, err := m.srcAdm.ListStartOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	teo, err := m.srcAdm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// Translate group offsets to destination cluster (in parallel due to MaxWaitMillis)
	nameConv := nameConverterFromTopicMappings(mappings)
	dstOffset := make([]int64, len(gcos))
	for i := range gcos {
		dstOffset[i] = unknownOffset
	}
	translateOffsetFn := func(i int, offset int64) error {
		g := gcos[i]

		o1, err := m.translateOffset(ctx, g.Topic, nameConv.ToDst(g.Topic), g.Partition, offset)
		if err != nil {
			return err
		}
		if o1 == unknownOffset {
			return errors.New("unknown offset")
		}

		m.log.Debugf("Consumer group migration: translated group '%s' topic '%s' partition %d offset %d to %d",
			g.Group, g.Topic, g.Partition, offset, o1)

		dstOffset[i] = o1
		return nil
	}
	for i, g := range gcos {
		o := g.At // consumer group offset

		// Load partition start and end offsets
		var (
			lo kadm.ListedOffset
			ok bool
		)

		lo, ok = tso.Lookup(g.Topic, g.Partition)
		if !ok {
			m.log.Errorf("Consumer group migration: group '%s' topic '%s' partition %d offset %d not found in source cluster - skipping",
				g.Group, g.Topic, g.Partition, o) // this should never happen
			continue
		}
		s := lo.Offset // topic partition start offset

		lo, ok = teo.Lookup(g.Topic, g.Partition)
		if !ok {
			m.log.Errorf("Consumer group migration: group '%s' topic '%s' partition %d offset %d not found in source cluster - skipping",
				g.Group, g.Topic, g.Partition, o) // this should never happen
			continue
		}
		e := lo.Offset // topic partition end offset

		// Ensure that `o` is in range `(s, e]`
		if o <= s {
			m.log.Infof("Consumer group migration: group '%s' topic '%s' partition %d start offset %d >= group offset %d - skipping",
				g.Group, g.Topic, g.Partition, s, o)
			continue
		}
		if o > e {
			m.log.Infof("Consumer group migration: group '%s' topic '%s' partition %d end offset %d < group offset %d - skipping",
				g.Group, g.Topic, g.Partition, e, o)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			if err := translateOffsetFn(i, o); err != nil {
				m.log.Errorf("Consumer group migration: group '%s' topic '%s' partition %d failed to translate offset %d to destination cluster: %v - skipping",
					g.Group, g.Topic, g.Partition, o, err)
				m.metrics.IncOffsetTranslationErrors(g.Group)
			} else {
				m.metrics.ObserveOffsetTranslationLatency(g.Group, time.Since(t0))
				m.metrics.IncOffsetsTranslated(g.Group)
			}
		}()
	}
	wg.Wait()

	// Merge offsets to commit for each group
	dstOffsets := m.dstAdm.FetchManyOffsets(ctx, extractGroupNames(gcos)...)
	offsetsToCommit := make(map[string]kadm.Offsets)
	offsetsToCommitCount := 0
	for i, gco := range gcos {
		o := dstOffset[i]

		// Skip invalid offsets, or offsets that failed to translate
		if o <= 0 {
			continue
		}

		g := gco.Group
		t := nameConv.ToDst(gco.Topic)
		p := gco.Partition

		// Do not rewind offset
		if cur, ok := dstOffsets[g].Fetched.Lookup(t, p); ok && cur.Err == nil && cur.At >= o {
			m.log.Debugf("Consumer group migration: group '%s' topic '%s' partition %d in destination is ahead of translated offset %d >= %d - skipping",
				g, t, p, cur.At, o)
			continue
		}

		if offsetsToCommit[g] == nil {
			offsetsToCommit[g] = make(kadm.Offsets)
		}
		if offsetsToCommit[g][t] == nil {
			offsetsToCommit[g][t] = make(map[int32]kadm.Offset)
		}
		offsetsToCommit[g][t][p] = kadm.Offset{
			Topic:       t,
			Partition:   p,
			At:          o,
			LeaderEpoch: -1,
			Metadata:    gco.Metadata,
		}
		offsetsToCommitCount += 1
	}
	if len(offsetsToCommit) == 0 {
		m.log.Infof("Consumer group migration: no offsets to commit")
		return nil
	}

	// Commit offsets (in parallel)
	type groupOffsets struct {
		Group string
		kadm.Offsets
	}
	committedOffsets := make([]groupOffsets, len(offsetsToCommit))
	var failedOffsets atomic.Int32

	idx := -1
	for g, offsets := range offsetsToCommit {
		idx += 1

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			m.log.Debugf("Consumer group migration: committing offsets for group '%s' %+v", g, offsets)

			t0 := time.Now()
			resp, err := m.dstAdm.CommitOffsets(ctx, g, offsets)
			if err != nil {
				m.log.Errorf("Consumer group migration: failed to update offsets for group '%s': %v", g, err)

				cnt := 0
				offsets.Each(func(_ kadm.Offset) {
					cnt += 1
					m.metrics.IncOffsetCommitErrors(g)
				})
				failedOffsets.Add(int32(cnt))

				return
			}

			commited := make(kadm.Offsets)
			cnt := 0
			failed := 0
			resp.Each(func(r kadm.OffsetResponse) {
				cnt += 1
				if r.Err != nil {
					m.log.Errorf("Consumer group migration: failed to update offset for group '%s' topic '%s' partition %d: %v",
						g, r.Topic, r.Partition, r.Err)
					failed += 1
					m.metrics.IncOffsetCommitErrors(g)
				} else {
					commited.Add(r.Offset)
					m.metrics.IncOffsetsCommitted(g)
				}
			})

			m.metrics.ObserveOffsetCommitLatency(g, time.Since(t0))

			m.log.Debugf("Consumer group migration: successfully committed %d of %d offsets for group '%s'",
				cnt-failed, cnt, g)

			committedOffsets[idx] = groupOffsets{Group: g, Offsets: commited}
			if failed > 0 {
				failedOffsets.Add(int32(failed))
			}
		}(idx)
	}
	wg.Wait()

	// Process commit responses and update committed offsets cache
	for _, offsets := range committedOffsets {
		g := offsets.Group
		offsets.Each(func(co kadm.Offset) {
			t := nameConv.ToSrc(co.Topic)
			p := co.Partition

			v, ok := m.commitedOffsets[g][t][p]
			if !ok {
				m.log.Errorf("Consumer group migration: failed to update offset for group '%s' topic '%s' partition %d: offset not found", g, t, p) // this should never happen
				return
			}
			v[1] = co.At
			m.commitedOffsets[g][t][p] = v
		})
	}

	m.log.Infof("Consumer group migration: successfully committed %d/%d offsets",
		offsetsToCommitCount-int(failedOffsets.Load()), offsetsToCommitCount)

	return nil
}

func (m *groupsMigrator) enabled() bool {
	return m.conf.Enabled && (m.srcAdm != nil || m.dstAdm != nil)
}

func (m *groupsMigrator) filterTopics(all []TopicMapping) []string {
	topics := make([]string, 0, len(all))
	for _, tm := range all {
		// Partition counts must match between source and destination clusters.
		// Otherwise, it is impossible to migrate the consumer groups.
		if tm.Src.Partitions != tm.Dst.Partitions {
			m.log.Infof("Consumer group migration: skipping topic '%s' with mismatched partition counts, source: %d, destination: %d",
				tm.Src.Topic, tm.Src.Partitions, tm.Dst.Partitions)
			continue
		}
		topics = append(topics, tm.Src.Topic)
	}
	return topics
}

// extractTopics takes a slice of GroupOffset and returns a slice of unique
// topic names. The order of topics in the returned slice is undefined.
func extractTopics(gcos []GroupOffset) []string {
	m := make(map[string]struct{}, len(gcos))
	for _, gco := range gcos {
		m[gco.Topic] = struct{}{}
	}

	topics := make([]string, 0, len(m))
	for t := range m {
		topics = append(topics, t)
	}
	return topics
}

func extractGroupNames(gcos []GroupOffset) []string {
	ss := make([]string, len(gcos))
	for i, gco := range gcos {
		ss[i] = gco.Group
	}
	return ss
}

func (m *groupsMigrator) fillTopicIDs(ctx context.Context, topics []string) error {
	var unknownTopics []string
	for _, t := range topics {
		if _, ok := m.topicIDs[t]; !ok {
			unknownTopics = append(unknownTopics, t)
		}
	}
	if len(unknownTopics) == 0 {
		return nil
	}

	details, err := m.srcAdm.ListTopics(ctx, unknownTopics...)
	if err != nil {
		return err
	}
	if err := details.Error(); err != nil {
		return err
	}

	for _, t := range unknownTopics {
		m.topicIDs[t] = details[t].ID
	}

	return nil
}

const unknownOffset int64 = -1

// translateOffset returns approximate commited offset in the destination
// cluster for a given commited offset in the source cluster. It is a special
// case of translateOffset function that is safe to use when we might point to
// the end of the partition.
//
// The function performs timestamp based offset translation. It reads the record
// timestamp of the PREVIOUS offset and then finds the first offset with the
// timestamp greater than or equal to the requested timestamp in the destination
// cluster.
//
// Caller must ensure that the provided offset is greater than the partition
// start offset. If offset translation fails, it returns unknownOffset (-1).
//
// NOTE: This method only works when timestamps are monotonically increasing.
func (m *groupsMigrator) translateOffset(
	ctx context.Context,
	srcTopic, dstTopic string,
	partition int32, offset int64,
) (int64, error) {
	// Read record timestamp for the PREVIOUS offset
	ts, err := readRecordTimestamp(ctx, m.src, srcTopic, m.topicIDs[srcTopic],
		partition, offset-1, m.conf.FetchTimeout)
	if err != nil {
		return unknownOffset, err
	}

	// List first offset with timestamp >= requested timestamp
	lo, err := m.dstAdm.ListOffsetsAfterMilli(ctx, ts.UnixMilli(), dstTopic)
	if err != nil {
		return unknownOffset, err
	}
	if err := lo.Error(); err != nil {
		return unknownOffset, err
	}

	tpo, ok := lo.Lookup(dstTopic, partition)
	if !ok || tpo.Offset == unknownOffset {
		m.log.Debugf("Consumer group migration: no offsets found for topic '%s' partition %d after timestamp %s",
			dstTopic, partition, ts)
		return unknownOffset, nil
	}

	// Handle offset translation based on timestamp matching.
	//
	// ListOffsetsAfterMilli returns the first offset with timestamp >= requested timestamp.
	// Since we queried for the timestamp of offset-1, we need to adjust the result:
	//
	// Case 1: Found timestamp > requested timestamp
	//   - The exact record wasn't found (may be deleted or destination has newer data)
	//   - Return the found offset as best approximation
	//
	// Case 2: Found timestamp == requested timestamp
	//   - We found a record with the same timestamp as the record at offset-1
	//   - Since ListOffsetsAfterMilli returns the FIRST offset with that timestamp,
	//     we need to add 1 to get the correct translated offset
	o1 := tpo.Offset
	if tpo.Timestamp == ts.UnixMilli() {
		o1 += 1
	}
	return o1, nil
}

// readRecordTimestamp sends a fetch request to the Redpanda cluster to read the
// timestamp of the record at the given topic, partition, and offset.
//
// The function returns the timestamp of the record at the given offset, or an
// error if the request fails or if the record is not found.
//
// The function does not retry the request if it fails, as the caller should
// handle retries according to their needs.
func readRecordTimestamp(
	ctx context.Context,
	client *kgo.Client,
	topic string,
	topicID kadm.TopicID,
	partition int32,
	offset int64,
	fetchTimeout time.Duration,
) (time.Time, error) {
	// Build fetch request
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = int32(fetchTimeout.Milliseconds()) // If data is not available we wait at most this duration
	req.MinBytes = 1
	req.MaxBytes = 1 // The response can exceed MaxBytes if the first record is larger than MaxBytes

	topicReq := kmsg.NewFetchRequestTopic()
	topicReq.Topic = topic
	topicReq.TopicID = topicID

	partitionReq := kmsg.NewFetchRequestTopicPartition()
	partitionReq.Partition = partition
	partitionReq.FetchOffset = offset

	topicReq.Partitions = append(topicReq.Partitions, partitionReq)
	req.Topics = append(req.Topics, topicReq)

	// Send fetch request and process response
	resp, err := client.Request(ctx, req)
	if err != nil {
		return time.Time{}, fmt.Errorf("fetch request failed: %w", err)
	}
	fetchResp, ok := resp.(*kmsg.FetchResponse)
	if !ok {
		return time.Time{}, fmt.Errorf("unexpected response type: %T", resp)
	}
	if len(fetchResp.Topics) == 0 {
		return time.Time{}, errors.New("no topics in response")
	}
	respTopic := &fetchResp.Topics[0]
	if len(respTopic.Partitions) == 0 {
		return time.Time{}, errors.New("no partitions in response")
	}
	respPartition := &respTopic.Partitions[0]
	if respPartition.ErrorCode != 0 {
		return time.Time{}, fmt.Errorf("partition error: %d", respPartition.ErrorCode)
	}

	// Extract record timestamp
	fp, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
		Partition: partition,
		Offset:    offset,
	}, respPartition, kgo.DefaultDecompressor(), nil)
	if fp.Err != nil {
		return time.Time{}, fmt.Errorf("processing partition failed: %w", fp.Err)
	}
	if len(fp.Records) == 0 {
		return time.Time{}, errors.New("no records in response")
	}
	if fp.Records[0].Offset != offset {
		return time.Time{}, fmt.Errorf("first record has offset %d, expected %d", fp.Records[0].Offset, offset)
	}
	return fp.Records[0].Timestamp, nil
}

type groupsMetrics struct {
	offsetsTranslated        *service.MetricCounter
	offsetTranslationErrors  *service.MetricCounter
	offsetTranslationLatency *service.MetricTimer
	offsetsCommitted         *service.MetricCounter
	offsetCommitErrors       *service.MetricCounter
	offsetCommitLatency      *service.MetricTimer
}

func newGroupsMetrics(m *service.Metrics) *groupsMetrics {
	return &groupsMetrics{
		offsetsTranslated:        m.NewCounter("redpanda_migrator_cg_offsets_translated_total", "group"),
		offsetTranslationErrors:  m.NewCounter("redpanda_migrator_cg_offset_translation_errors_total", "group"),
		offsetTranslationLatency: m.NewTimer("redpanda_migrator_cg_offset_translation_latency_ns", "group"),
		offsetsCommitted:         m.NewCounter("redpanda_migrator_cg_offsets_committed_total", "group"),
		offsetCommitErrors:       m.NewCounter("redpanda_migrator_cg_offset_commit_errors_total", "group"),
		offsetCommitLatency:      m.NewTimer("redpanda_migrator_cg_offset_commit_latency_ns", "group"),
	}
}

func (gm *groupsMetrics) IncOffsetsTranslated(group string) {
	if gm == nil {
		return
	}
	gm.offsetsTranslated.Incr(1, group)
}

func (gm *groupsMetrics) IncOffsetTranslationErrors(group string) {
	if gm == nil {
		return
	}
	gm.offsetTranslationErrors.Incr(1, group)
}

func (gm *groupsMetrics) ObserveOffsetTranslationLatency(group string, d time.Duration) {
	if gm == nil {
		return
	}
	gm.offsetTranslationLatency.Timing(d.Nanoseconds(), group)
}

func (gm *groupsMetrics) IncOffsetsCommitted(group string) {
	if gm == nil {
		return
	}
	gm.offsetsCommitted.Incr(1, group)
}

func (gm *groupsMetrics) IncOffsetCommitErrors(group string) {
	if gm == nil {
		return
	}
	gm.offsetCommitErrors.Incr(1, group)
}

func (gm *groupsMetrics) ObserveOffsetCommitLatency(group string, d time.Duration) {
	if gm == nil {
		return
	}
	gm.offsetCommitLatency.Timing(d.Nanoseconds(), group)
}
