package migrator_test

import (
	"testing"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

func TestIntegrationTopicMigratorSyncConfig(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: topic with configs is created in source cluster")
	const topic = "topic-with-configs"
	configs := map[string]*string{
		"retention.ms": ptr.String("1500"),
	}
	src.CreateTopicWithConfigs(topic, configs)

	t.Log("When: InitKnownTopics is called")
	m := migrator.NewTopicMigratorForTesting(t, migrator.TopicMigratorConfig{})
	assert.NoError(t, m.Sync(t.Context(), src.Admin, dst.Admin, func() []string {
		return []string{topic}
	}))

	t.Log("Then: Topic is created in destination cluster with configs")
	assert.Equal(t, ptr.String("1500"), dst.TopicConfig(topic, "retention.ms"))
}

func TestIntegrationTopicMigratorSyncACLs(t *testing.T) {
	integration.CheckSkip(t)

	hasACL := func(t *testing.T, cluster EmbeddedRedpandaCluster, topic, principal string, perm kmsg.ACLPermissionType, op kmsg.ACLOperation) bool {
		acls, err := cluster.DescribeTopicACLs(topic)
		if err != nil {
			t.Logf("Failed to describe ACLs (treating as not found): %v", err)
			return false
		}
		for _, a := range acls {
			t.Logf("Found ACL: %v", a)

			if a.Principal == principal && a.Permission == perm && a.Operation == op {
				return true
			}
		}
		return false
	}

	tests := []struct {
		name   string
		setup  func(src EmbeddedRedpandaCluster)
		assert func(t *testing.T, dst EmbeddedRedpandaCluster)
	}{
		{
			name: "allow_describe",
			setup: func(src EmbeddedRedpandaCluster) {
				src.CreateACLAllow(migratorTestTopic, "User:dummy", kmsg.ACLOperationDescribe)
			},
			assert: func(t *testing.T, dst EmbeddedRedpandaCluster) {
				assert.Eventually(t, func() bool {
					return hasACL(t, dst, migratorTestTopic, "User:dummy", kmsg.ACLPermissionTypeAllow, kmsg.ACLOperationDescribe)
				}, redpandaTestWaitTimeout, 200*time.Millisecond)
			},
		},
		{
			name: "downgrade_all_to_read",
			setup: func(src EmbeddedRedpandaCluster) {
				src.CreateACLAllow(migratorTestTopic, "User:dummy", kmsg.ACLOperationAll)
			},
			assert: func(t *testing.T, dst EmbeddedRedpandaCluster) {
				assert.Eventually(t, func() bool {
					return hasACL(t, dst, migratorTestTopic, "User:dummy", kmsg.ACLPermissionTypeAllow, kmsg.ACLOperationRead)
				}, redpandaTestWaitTimeout, 200*time.Millisecond)
			},
		},
		{
			name: "skip_allow_write",
			setup: func(src EmbeddedRedpandaCluster) {
				src.CreateACLAllow(migratorTestTopic, "User:dummy", kmsg.ACLOperationWrite)
			},
			assert: func(t *testing.T, dst EmbeddedRedpandaCluster) {
				assert.Never(t, func() bool {
					return hasACL(t, dst, migratorTestTopic, "User:dummy", kmsg.ACLPermissionTypeAllow, kmsg.ACLOperationWrite)
				}, redpandaTestOpTimeout, 200*time.Millisecond)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Log("Given: Redpanda clusters")
			src, dst := startRedpandaSourceAndDestination(t)

			t.Log("And: ACLs are set up")
			tc.setup(src)

			t.Log("When: InitKnownTopics is called")
			m := migrator.NewTopicMigratorForTesting(t, migrator.TopicMigratorConfig{SyncACLs: true})
			assert.NoError(t, m.Sync(t.Context(), src.Admin, dst.Admin, func() []string {
				return []string{migratorTestTopic}
			}))

			t.Log("Then: Expected ACLs are set up")
			tc.assert(t, dst)
		})
	}
}

func TestIntegrationTopicMigratorIdempotentSyncIdempotence(t *testing.T) {
	integration.CheckSkip(t)

	defaultTopic := func() []string {
		return []string{migratorTestTopic}
	}

	hasTopic := func(adm *kadm.Client, topic string) bool {
		topics, err := adm.ListTopics(t.Context(), topic)
		require.NoError(t, err)
		_, ok := topics[topic]
		return ok
	}

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("When: Sync is called first time")
	m0 := migrator.NewTopicMigratorForTesting(t, migrator.TopicMigratorConfig{})
	require.NoError(t, m0.Sync(t.Context(), src.Admin, dst.Admin, defaultTopic))

	t.Log("Then: topic exists in destination with expected configs")
	assert.True(t, hasTopic(dst.Admin, migratorTestTopic))

	t.Log("When: Sync is called second time")
	m1 := migrator.NewTopicMigratorForTesting(t, migrator.TopicMigratorConfig{})
	require.NoError(t, m1.Sync(t.Context(), src.Admin, dst.Admin, defaultTopic))

	t.Log("Then: nothing changes")
}

func TestIntegrationTopicMigratorPartitionGrowth(t *testing.T) {
	integration.CheckSkip(t)

	partitionCount := func(adm *kadm.Client, topic string) int {
		topics, err := adm.ListTopics(t.Context(), topic)
		require.NoError(t, err)
		topicDetail, ok := topics[topic]
		require.True(t, ok, "topic not found")
		return len(topicDetail.Partitions)
	}

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: destination topic exists with 1 partition")
	const testTopic = "partition-growth-topic"
	_, err := dst.Admin.CreateTopic(t.Context(), 1, 1, nil, testTopic)
	require.NoError(t, err)
	assert.Equal(t, 1, partitionCount(dst.Admin, testTopic))

	t.Log("And: source topic exists with 2 partitions")
	_, err = src.Admin.CreateTopic(t.Context(), 2, 1, nil, testTopic)
	require.NoError(t, err)
	assert.Equal(t, 2, partitionCount(src.Admin, testTopic))

	t.Log("When: Sync is called")
	m := migrator.NewTopicMigratorForTesting(t, migrator.TopicMigratorConfig{})
	require.NoError(t, m.Sync(t.Context(), src.Admin, dst.Admin, func() []string {
		return []string{testTopic}
	}))

	t.Log("Then: destination topic partition count increased to 2")
	assert.Equal(t, 2, partitionCount(dst.Admin, testTopic))
}
