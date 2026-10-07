// Copyright 2026 Redpanda Data, Inc.
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

package kafka_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// assignmentGroup is a consumer group of redpanda inputs used to observe
// which partition assignment strategy the broker selects and how partitions
// are divided among members.
type assignmentGroup struct {
	t          *testing.T
	brokerAddr string
	group      string
	topics     map[string]int32 // topic name to partition count
	adm        *kadm.Client
}

func newAssignmentGroup(t *testing.T, partitions ...int32) *assignmentGroup {
	t.Helper()

	brokerAddr, _ := sharedRedpanda(t)
	id := fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())

	g := &assignmentGroup{
		t:          t,
		brokerAddr: brokerAddr,
		group:      "group-" + id,
		topics:     map[string]int32{},
	}
	for i, n := range partitions {
		topicID := fmt.Sprintf("%s-%d", id, i)
		require.NoError(t, createKafkaTopic(t.Context(), brokerAddr, topicID, n))
		g.topics["topic-"+topicID] = n
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	g.adm = kadm.NewClient(cl)

	return g
}

// startMember runs a redpanda input stream that joins the group with the
// given strategies, and returns a function that stops it. Members still
// running when the test ends are stopped during cleanup.
func (g *assignmentGroup) startMember(strategies ...string) (stop func()) {
	g.t.Helper()

	topics := make([]string, 0, len(g.topics))
	for topic := range g.topics {
		topics = append(topics, topic)
	}

	strategyYAML := ""
	if len(strategies) > 0 {
		strategyYAML = fmt.Sprintf("partition_assignment_strategy: [ %s ]", strings.Join(strategies, ", "))
	}

	sb := service.NewStreamBuilder()
	require.NoError(g.t, sb.SetYAML(fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: %s
    %s
output:
  drop: {}
`, g.brokerAddr, strings.Join(topics, ", "), g.group, strategyYAML)))
	require.NoError(g.t, sb.SetLoggerYAML(`level: OFF`))

	stream, err := sb.Build()
	require.NoError(g.t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = stream.Run(runCtx)
	}()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		if err := stream.Stop(stopCtx); err != nil {
			cancel()
		}
		<-done
		cancel()
	}
	g.t.Cleanup(stop)
	return stop
}

// assignment maps member ID to topic to the number of partitions assigned.
type assignment map[string]map[string]int

// waitStable waits until the group is stable with the given number of
// members and every partition of every topic is assigned, then returns the
// selected protocol and the assignment.
func (g *assignmentGroup) waitStable(members int) (protocol string, assigned assignment) {
	g.t.Helper()

	var total int
	for _, n := range g.topics {
		total += int(n)
	}

	require.EventuallyWithT(g.t, func(c *assert.CollectT) {
		described, err := g.adm.DescribeGroups(g.t.Context(), g.group)
		if !assert.NoError(c, err) {
			return
		}
		dg := described[g.group]
		if !assert.NoError(c, dg.Err) {
			return
		}
		assert.Equal(c, "Stable", dg.State)
		if !assert.Len(c, dg.Members, members) {
			return
		}

		got := assignment{}
		var count int
		for _, m := range dg.Members {
			ca, ok := m.Assigned.AsConsumer()
			if !assert.True(c, ok, "member %v has no consumer assignment", m.MemberID) {
				return
			}
			perTopic := map[string]int{}
			for _, at := range ca.Topics {
				perTopic[at.Topic] += len(at.Partitions)
				count += len(at.Partitions)
			}
			got[m.MemberID] = perTopic
		}
		if !assert.Equal(c, total, count, "not all partitions are assigned yet") {
			return
		}

		protocol, assigned = dg.Protocol, got
	}, 2*time.Minute, 500*time.Millisecond)

	return protocol, assigned
}

// requireEvenPerTopic asserts that each topic's partitions are split across
// members with at most one partition of difference.
func (g *assignmentGroup) requireEvenPerTopic(assigned assignment) {
	g.t.Helper()

	for topic, n := range g.topics {
		lowest, highest := int(n), 0
		for _, perTopic := range assigned {
			lowest = min(lowest, perTopic[topic])
			highest = max(highest, perTopic[topic])
		}
		assert.LessOrEqual(g.t, highest-lowest, 1, "topic %v (%d partitions) is unevenly assigned: %v", topic, n, assigned)
	}
}

func TestIntegrationRedpandaPartitionAssignmentStrategy(t *testing.T) {
	integration.CheckSkip(t)

	for _, strategy := range []string{"range", "roundrobin", "sticky", "cooperative-sticky"} {
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()

			g := newAssignmentGroup(t, 10, 4, 7)
			for range 3 {
				g.startMember(strategy)
			}

			protocol, assigned := g.waitStable(3)
			assert.Equal(t, strategy, protocol)
			if strategy == "range" || strategy == "roundrobin" {
				g.requireEvenPerTopic(assigned)
			}
		})
	}

	t.Run("default_is_cooperative_sticky", func(t *testing.T) {
		t.Parallel()

		g := newAssignmentGroup(t, 4)
		g.startMember()
		g.startMember()

		protocol, _ := g.waitStable(2)
		assert.Equal(t, "cooperative-sticky", protocol)
	})

	t.Run("preference_order", func(t *testing.T) {
		t.Parallel()

		g := newAssignmentGroup(t, 4)
		g.startMember("range", "roundrobin")
		g.startMember("roundrobin", "range")
		g.startMember("roundrobin", "range")

		protocol, _ := g.waitStable(3)
		assert.Equal(t, "roundrobin", protocol, "the broker should select the strategy most members prefer")
	})
}

func TestIntegrationRedpandaPartitionAssignmentMigration(t *testing.T) {
	integration.CheckSkip(t)

	t.Run("no_common_strategy_is_rejected", func(t *testing.T) {
		t.Parallel()

		g := newAssignmentGroup(t, 4)
		g.startMember("cooperative-sticky")
		protocol, _ := g.waitStable(1)
		require.Equal(t, "cooperative-sticky", protocol)

		g.startMember("range")
		assert.Never(t, func() bool {
			described, err := g.adm.DescribeGroups(t.Context(), g.group)
			return err == nil && len(described[g.group].Members) > 1
		}, 15*time.Second, 500*time.Millisecond, "a member sharing no strategy with the group must not join it")
	})

	t.Run("two_step_rolling_update", func(t *testing.T) {
		t.Parallel()

		g := newAssignmentGroup(t, 10, 4, 7)
		stops := make([]func(), 3)
		for i := range stops {
			stops[i] = g.startMember("cooperative-sticky")
		}
		protocol, _ := g.waitStable(3)
		require.Equal(t, "cooperative-sticky", protocol)

		// Step one: list the old strategy first and the new one second. The
		// group keeps the old strategy throughout.
		for i := range stops {
			stops[i]()
			stops[i] = g.startMember("cooperative-sticky", "range")
			protocol, _ = g.waitStable(3)
			require.Equal(t, "cooperative-sticky", protocol, "after replacing member %d in step one", i)
		}

		// Step two: list only the new strategy. Every member still shares a
		// strategy with the group, so none are rejected.
		for i := range stops {
			stops[i]()
			stops[i] = g.startMember("range")
			g.waitStable(3)
		}

		protocol, assigned := g.waitStable(3)
		assert.Equal(t, "range", protocol)
		g.requireEvenPerTopic(assigned)
	})
}
