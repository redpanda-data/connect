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

package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type partLag struct {
	member string
	lag    int64
	err    error
}

func groupLags(lags map[string]map[int32]partLag) kadm.DescribedGroupLags {
	return groupLagsInState(groupStateStable, lags)
}

// groupLagsInState builds a described group lag whose members are every
// member named by a partition, plus "me", since a client that has joined is
// listed even when it holds no partitions.
func groupLagsInState(state string, lags map[string]map[int32]partLag) kadm.DescribedGroupLags {
	gl := kadm.GroupLag{}
	members := map[string]struct{}{"me": {}}
	for topic, parts := range lags {
		gl[topic] = map[int32]kadm.GroupMemberLag{}
		for p, pl := range parts {
			ml := kadm.GroupMemberLag{Topic: topic, Partition: p, Lag: pl.lag, Err: pl.err}
			if pl.member != "" {
				ml.Member = &kadm.DescribedGroupMember{MemberID: pl.member}
				members[pl.member] = struct{}{}
			}
			gl[topic][p] = ml
		}
	}
	described := kadm.DescribedGroupLag{Group: "g", State: state, Lag: gl}
	for m := range members {
		described.Members = append(described.Members, kadm.DescribedGroupMember{MemberID: m})
	}
	return kadm.DescribedGroupLags{"g": described}
}

// fetching returns a fetcher that serves the given lag.
func fetching(lags kadm.DescribedGroupLags) groupLagFetcher {
	return func(context.Context) (kadm.DescribedGroupLags, error) { return lags, nil }
}

// gaugeRecorder records every value set on a gauge.
type gaugeRecorder struct {
	mut  sync.Mutex
	sets []int64
}

func (g *gaugeRecorder) Set(v int64, _ ...string) {
	g.mut.Lock()
	defer g.mut.Unlock()
	g.sets = append(g.sets, v)
}

func (g *gaugeRecorder) values() []int64 {
	g.mut.Lock()
	defer g.mut.Unlock()
	return slices.Clone(g.sets)
}

func newTestConsumerLag(t *testing.T, fetch groupLagFetcher) (*ConsumerLag, *gaugeRecorder) {
	t.Helper()
	pending := &gaugeRecorder{}
	cl := newConsumerLag(fetch, func() string { return "me" }, service.MockResources().Logger(),
		&gaugeRecorder{}, pending, time.Hour)
	return cl, pending
}

func TestConsumerLagSnapshot(t *testing.T) {
	cl, pending := newTestConsumerLag(t, nil)
	assert.Zero(t, cl.Load("a", 0))

	cl.apply(groupLags(map[string]map[int32]partLag{
		"a": {0: {member: "me", lag: 5}, 1: {member: "me", lag: 7}, 2: {member: "other", lag: 100}},
		"b": {0: {member: "me", lag: -3}}, // franz-go reports a negative lag for an unknown offset
		"c": {0: {lag: 50}},               // a committed offset on a partition no member holds
	}), "me")
	assert.Equal(t, []int64{12}, pending.values(), "the pending gauge is this member's share")
	assert.Equal(t, int64(7), cl.Load("a", 1))
	assert.Equal(t, int64(100), cl.Load("a", 2), "per partition lag covers the whole group")
	assert.Zero(t, cl.Load("b", 0))

	// A partition that left the group drops out of the snapshot and its gauge
	// series is zeroed.
	gauge := cl.gauge.(*gaugeRecorder)
	before := len(gauge.values())
	cl.apply(groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 2}}}), "me")
	assert.Equal(t, []int64{12, 2}, pending.values())
	assert.Zero(t, cl.Load("a", 1))
	assert.Len(t, gauge.values(), before+1+4, "one set for a/0 and four zeroes for the departed partitions")

	// An empty group is a known zero, not an unknown.
	cl.apply(kadm.DescribedGroupLags{}, "me")
	assert.Equal(t, []int64{12, 2, 0}, pending.values())
}

func TestConsumerLagErrorsKeepLastSnapshot(t *testing.T) {
	cl, pending := newTestConsumerLag(t, nil)
	cl.apply(groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 9}}}), "me")

	// A describe error on the group leaves everything alone.
	bad := groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 0}}})
	g := bad["g"]
	g.DescribeErr = errors.New("coordinator loading")
	bad["g"] = g
	cl.fetch = fetching(bad)
	cl.refresh(t.Context())
	assert.Equal(t, []int64{9}, pending.values())
	assert.Equal(t, int64(9), cl.Load("a", 0))
}

func TestConsumerLagPartitionErrors(t *testing.T) {
	cl, pending := newTestConsumerLag(t, nil)

	// An offset error on another member's partition, such as a committed
	// offset on a deleted topic, is left out and doesn't affect our total.
	cl.apply(groupLags(map[string]map[int32]partLag{
		"a":    {0: {member: "me", lag: 6}},
		"gone": {0: {member: "other", lag: -1, err: errors.New("unknown topic")}},
	}), "me")
	assert.Equal(t, []int64{6}, pending.values())
	assert.Zero(t, cl.Load("gone", 0))

	// On one of our own partitions it makes the total unknown, so the pending
	// gauge keeps its last value, while the other partitions' lag is still
	// refreshed.
	cl.apply(groupLags(map[string]map[int32]partLag{
		"a": {0: {member: "me", lag: 8}, 1: {member: "me", lag: -1, err: errors.New("list offsets")}},
	}), "me")
	assert.Equal(t, []int64{6}, pending.values())
	assert.Equal(t, int64(8), cl.Load("a", 0))
}

func TestConsumerLagUnknownWhileRebalancing(t *testing.T) {
	cl, pending := newTestConsumerLag(t, nil)

	cl.apply(groupLagsInState("PreparingRebalance", map[string]map[int32]partLag{
		"a": {0: {lag: 1000}},
	}), "me")
	assert.Empty(t, pending.values(), "no member holds assignments during a rebalance")
	assert.Equal(t, int64(1000), cl.Load("a", 0), "per partition lag is still published")

	// Before this client has joined it has no member ID.
	cl.apply(groupLags(map[string]map[int32]partLag{"a": {0: {member: "someone", lag: 5}}}), "")
	assert.Empty(t, pending.values())

	// A member ID the broker handed out before the join completed is not
	// among the group's members yet, so a stable group holding no partition
	// for it is not a known zero.
	notYetJoined := groupLags(map[string]map[int32]partLag{"a": {0: {member: "someone", lag: 5}}})
	g := notYetJoined["g"]
	g.Members = []kadm.DescribedGroupMember{{MemberID: "someone"}}
	notYetJoined["g"] = g
	cl.apply(notYetJoined, "me")
	assert.Empty(t, pending.values())

	// A member ID that changed between the fetch and the apply means the
	// described assignments are not ours any more.
	ids := []string{"me", "me2"}
	cl.memberID = func() string { id := ids[0]; ids = ids[1:]; return id }
	cl.fetch = fetching(groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 5}}}))
	cl.refresh(t.Context())
	assert.Empty(t, pending.values())
}

func TestConsumerLagRefreshKeepsLastOnFetchError(t *testing.T) {
	var fail bool
	fetch := func(context.Context) (kadm.DescribedGroupLags, error) {
		if fail {
			return nil, errors.New("nope")
		}
		return groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 9}}}), nil
	}
	cl, pending := newTestConsumerLag(t, fetch)

	cl.refresh(t.Context())
	assert.Equal(t, []int64{9}, pending.values())

	fail = true
	cl.refresh(t.Context())
	assert.Equal(t, []int64{9}, pending.values(), "a failed refresh changes nothing")
	assert.Equal(t, int64(9), cl.Load("a", 0))
}

func TestConsumerLagStartRefreshesImmediately(t *testing.T) {
	fetch := func(context.Context) (kadm.DescribedGroupLags, error) {
		return groupLags(map[string]map[int32]partLag{"a": {0: {member: "me", lag: 3}}}), nil
	}
	cl, pending := newTestConsumerLag(t, fetch) // an hour between periodic refreshes
	cl.Start()
	cl.Stop() // waits for the start-up refresh

	assert.Equal(t, []int64{3, 0}, pending.values(), "an immediate refresh, then Stop zeroes the gauge")
}
