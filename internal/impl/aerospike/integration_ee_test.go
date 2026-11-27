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

package aerospike

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These exercise the behaviour a single-node Community server cannot reach:
// durable deletes, replica placement, strong consistency, access control and
// TLS. They need an Enterprise cluster with replication-factor 2, an AP
// namespace `test` and a strong-consistency namespace `sc`, addressed by
// AEROSPIKE_EE_HOSTS.
//
// Local 3-container setup (license key required): testdata/ee/README.md
// (as-ee-1/2 for AEROSPIKE_EE_HOSTS, as-ee-sec for AEROSPIKE_SEC_HOST / TLS).
const (
	eeAPNamespace = "test"
	eeSCNamespace = "sc"
	eeSet         = "rpa_ee"
)

func eeHosts(t *testing.T) string {
	t.Helper()
	hosts := os.Getenv("AEROSPIKE_EE_HOSTS")
	if hosts == "" {
		t.Skip("AEROSPIKE_EE_HOSTS is unset; Enterprise tests need a licensed multi-node cluster")
	}
	return hosts
}

// eeWriter builds an output against the Enterprise cluster. The caller supplies
// the namespace so strong-consistency tests can target `sc`.
func eeWriter(t *testing.T, namespace, extraYAML string) (*aerospikeWriter, *as.Client) {
	t.Helper()

	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + namespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	client, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	if namespace == eeSCNamespace {
		ensureSCRoster(t, client)
	}
	require.NoError(t, client.Truncate(nil, namespace, eeSet, nil))
	// Truncate returns when the command is accepted, not when the set is empty.
	// A write in that window is removed again, so the following read misses a
	// record the batch call just reported as stored.
	waitUntilSetEmpty(t, client, namespace, eeSet)
	return w, client
}

func eeLookup(t *testing.T, namespace, extraYAML string) *lookupProcessor {
	t.Helper()

	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + namespace + `
set: ` + eeSet + `
key: '${! json("id") }'
` + extraYAML

	p := newTestProcessor(t, yaml)
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

func quoteHosts(csv string) string {
	parts := strings.Split(csv, ",")
	for i, p := range parts {
		parts[i] = `"` + strings.TrimSpace(p) + `"`
	}
	return strings.Join(parts, ", ")
}

func eeRead(t *testing.T, client *as.Client, namespace, id string) *as.Record {
	t.Helper()
	key, err := as.NewKey(namespace, eeSet, id)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	if asErr != nil && asErr.Matches(2 /* KEY_NOT_FOUND_ERROR */) {
		return nil
	}
	require.NoError(t, asErr)
	return rec
}

// ensureSCRoster makes namespace sc writable. Two separate failures both
// surface as "not connected":
//
//   - the saved roster names node ids that are gone, so ns_cluster_size stays 0
//   - the roster matches, but a memory namespace restart has no stored
//     partitions, so every partition is dead until each node is revived
//
// Revive is safe here only because this namespace is memory-backed test data.
func ensureSCRoster(t *testing.T, client *as.Client) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if scRosterReady(client) {
			return
		}
		stageSCRoster(client)
		time.Sleep(time.Second)
	}
	t.Fatal("namespace sc roster does not include the live nodes")
}

func scRosterReady(client *as.Client) bool {
	nodes := client.GetNodes()
	if len(nodes) < 2 {
		return false
	}
	for _, node := range nodes {
		size, err := namespaceField(node, eeSCNamespace, "ns_cluster_size")
		if err != nil || size != "2" {
			return false
		}
		dead, err := namespaceField(node, eeSCNamespace, "dead_partitions")
		if err != nil || dead != "0" {
			return false
		}
		body, err := infoValue(node, "roster:namespace="+eeSCNamespace)
		if err != nil {
			return false
		}
		active, observed := rosterNodeSets(body)
		if nodeCount(observed) != 2 || active != observed {
			return false
		}
	}
	return true
}

func stageSCRoster(client *as.Client) {
	nodes := client.GetNodes()
	var principal *as.Node
	var active, observed string
	dead := false
	for _, node := range nodes {
		id, err := infoValue(node, "node")
		if err != nil {
			return
		}
		stats, err := infoValue(node, "statistics")
		if err != nil {
			return
		}
		if strings.EqualFold(id, statField(stats, "cluster_principal")) {
			principal = node
		}
		body, err := infoValue(node, "roster:namespace="+eeSCNamespace)
		if err != nil {
			return
		}
		rosterActive, rosterObserved := rosterNodeSets(body)
		if rosterObserved != "" {
			active, observed = rosterActive, rosterObserved
		}
		deadParts, err := namespaceField(node, eeSCNamespace, "dead_partitions")
		if err != nil {
			return
		}
		if deadParts != "0" {
			dead = true
		}
	}
	// A one-node observed set is the cluster still forming. Staging it would
	// lock the namespace at replication factor 1.
	if principal == nil || nodeCount(observed) != 2 {
		return
	}
	changed := false
	if active != observed {
		if _, err := infoValue(principal, "roster-set:namespace="+eeSCNamespace+";nodes="+observed); err != nil {
			return
		}
		changed = true
	}
	if dead {
		// Every node has to accept revive. Sending it only to the principal
		// leaves the other node's partitions dead.
		for _, node := range nodes {
			if _, err := infoValue(node, "revive:namespace="+eeSCNamespace); err != nil {
				return
			}
		}
		changed = true
	}
	if changed {
		_, _ = infoValue(principal, "recluster:")
	}
}

// infoValue runs one info command. Callers in the roster retry treat an error
// as "not ready yet" and try again. Failing the test here would turn one
// brief error while the cluster is forming into a hard failure.
func infoValue(node *as.Node, command string) (string, error) {
	info, err := node.RequestInfo(as.NewInfoPolicy(), command)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(info[command]), nil
}

func namespaceField(node *as.Node, namespace, field string) (string, error) {
	body, err := infoValue(node, "namespace/"+namespace)
	if err != nil {
		return "", err
	}
	return statField(body, field), nil
}

func statField(body, name string) string {
	for part := range strings.FieldsFuncSeq(body, func(r rune) bool { return r == ';' || r == ':' }) {
		key, value, ok := strings.Cut(part, "=")
		if ok && key == name {
			return value
		}
	}
	return ""
}

// rosterNodeSets returns the active roster and the observed nodes, each as a
// sorted comma-separated set. "null" and empty entries are dropped.
func rosterNodeSets(body string) (active, observed string) {
	return nodeSet(statField(body, "roster")), nodeSet(statField(body, "observed_nodes"))
}

func nodeCount(set string) int {
	if set == "" {
		return 0
	}
	return strings.Count(set, ",") + 1
}

func nodeSet(raw string) string {
	parts := strings.Split(raw, ",")
	kept := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || strings.EqualFold(part, "null") {
			continue
		}
		kept = append(kept, strings.ToUpper(part))
	}
	if len(kept) == 0 {
		return ""
	}
	slices.Sort(kept)
	return strings.Join(kept, ",")
}

func TestRosterNodeSets(t *testing.T) {
	active, observed := rosterNodeSets("roster=null:pending_roster=BB91DDA83FEAFC6,BB917465FFBF3E6:observed_nodes=BB917465FFBF3E6,BB91DDA83FEAFC6")
	assert.Empty(t, active)
	assert.Equal(t, "BB917465FFBF3E6,BB91DDA83FEAFC6", observed)

	active, observed = rosterNodeSets("roster=bb917465ffbf3e6,bb91dda83feafc6:observed_nodes=BB91DDA83FEAFC6,BB917465FFBF3E6")
	assert.Equal(t, observed, active)
	assert.NotEmpty(t, observed)
}

// nsStat sums a namespace statistic across every node, so replica-side effects
// are counted rather than only whatever the master happened to do.
func nsStat(t *testing.T, client *as.Client, namespace, stat string) int64 {
	t.Helper()

	var total int64
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "namespace/"+namespace)
		require.NoError(t, err)
		for field := range strings.SplitSeq(info["namespace/"+namespace], ";") {
			name, value, ok := strings.Cut(field, "=")
			if !ok || name != stat {
				continue
			}
			n, err := strconv.ParseInt(value, 10, 64)
			require.NoError(t, err)
			total += n
		}
	}
	return total
}

// waitUntilSetEmpty blocks until every node has finished truncating this set.
// objects and tombstones are per node; with replication-factor 2 both copies
// have to be gone before a new write is safe from the truncate cutoff.
func waitUntilSetEmpty(t *testing.T, client *as.Client, namespace, set string) {
	t.Helper()

	cmd := "sets/" + namespace + "/" + set
	require.Eventually(t, func() bool {
		nodes := client.GetNodes()
		if len(nodes) == 0 {
			return false
		}
		for _, node := range nodes {
			info, err := node.RequestInfo(as.NewInfoPolicy(), cmd)
			if err != nil {
				return false
			}
			raw := info[cmd]
			if raw == "" {
				continue
			}
			fields := map[string]string{}
			for field := range strings.SplitSeq(raw, ":") {
				name, value, ok := strings.Cut(field, "=")
				if ok {
					fields[name] = value
				}
			}
			if fields["truncating"] == "true" || fields["objects"] != "0" || fields["tombstones"] != "0" {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond,
		"truncate of %s.%s did not finish on every node", namespace, set)
}

// settledStat waits for a namespace statistic to stop moving. Truncate reclaims
// records in the background, so a count read straight after one drifts under
// the test and turns tombstone assertions into coin flips.
func settledStat(t *testing.T, client *as.Client, namespace, stat string) int64 {
	t.Helper()

	last := nsStat(t, client, namespace, stat)
	for range 50 {
		time.Sleep(100 * time.Millisecond)
		current := nsStat(t, client, namespace, stat)
		if current == last {
			return current
		}
		last = current
	}
	t.Fatalf("%v.%v never settled", namespace, stat)
	return 0
}

func TestEEClusterIsMultiNode(t *testing.T) {
	_, client := eeWriter(t, eeAPNamespace, "")

	nodes := client.GetNodes()
	require.GreaterOrEqual(t, len(nodes), 2, "these tests are meaningless without replicas")

	// Replication factor has to be above one or commit_level and replica are
	// both no-ops no matter what the tests assert.
	assert.GreaterOrEqual(t, nsStat(t, client, eeAPNamespace, "effective_replication_factor"), int64(2)*int64(len(nodes)))
}

// TTL 20s + nsup-period 10: write, see the record, wait until NSUP drops it.
// Long enough to SELECT PK ttl-gone from as-ee-tools while the test runs.
func TestEETTLExpiresRemovesRecord(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "ttl: 20s\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ttl-gone","v":"temp"}`),
	}))
	rec := eeRead(t, client, eeAPNamespace, "ttl-gone")
	require.NotNil(t, rec, "record must exist before NSUP runs")
	assert.InDelta(t, 20, rec.Expiration, 5)

	// 20s TTL plus nsup-period 10, with slack so NSUP can delete it.
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if eeRead(t, client, eeAPNamespace, "ttl-gone") == nil {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("record still present after TTL + nsup-period; check namespace test has nsup-period 10")
}

// TTL interpolated from a JSON field "24H": the record must land with an
// expiration close to 86400 seconds. We do not wait for it to expire.
func TestEETTLInterpolated24HKeepsRecord(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "ttl: '${! json(\"ttl\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ttl-24h","v":"persistent","ttl":"24H"}`),
	}))

	rec := eeRead(t, client, eeAPNamespace, "ttl-24h")
	require.NotNil(t, rec, "record must exist immediately after write")
	assert.InDelta(t, 86400, rec.Expiration, 60, "expiration must be approximately 24 h (86400 s)")
}

// An invalid TTL token ("24X") must be rejected per-message without sending
// anything to Aerospike. The batch error must be indexed and the record must
// not appear on the cluster.
func TestEETTLInvalidValueIsRejected(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "ttl: '${! json(\"ttl\") }'\n")

	batch := service.MessageBatch{msg(t, `{"id":"ttl-bad","v":"temp","ttl":"24X"}`)}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err, "an invalid TTL value must produce an error")
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "ttl",
		"the indexed error must name the offending field")
	assert.Nil(t, eeRead(t, client, eeAPNamespace, "ttl-bad"),
		"the rejected message must not have been stored")
}

// A record written with ttl=never must survive at least one full NSUP cycle.
// The record stays until the next test truncates this set. Storage is memory,
// so that leftover is not durable.
// A short-lived control record (12 s TTL) acts as a canary: once NSUP removes
// it we know the subsystem has run and the immortal record was deliberately
// spared. With nsup-period 10 the control is gone within 12+10=22 s worst
// case; we poll for up to 60 s to give the server margin.
func TestEETTLNeverSurvivesNSUP(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "ttl: '${! json(\"ttl\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ttl-never","v":"immortal","ttl":"never"}`),
	}))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ttl-ctrl","v":"ephemeral","ttl":"12s"}`),
	}))

	require.NotNil(t, eeRead(t, client, eeAPNamespace, "ttl-never"),
		"never record must exist before any NSUP cycle")
	require.NotNil(t, eeRead(t, client, eeAPNamespace, "ttl-ctrl"),
		"control record must exist before NSUP removes it")

	// Block until NSUP demonstrably removes the control record. Once it is
	// gone we know at least one NSUP pass has completed since the writes.
	assert.Eventually(t, func() bool {
		return eeRead(t, client, eeAPNamespace, "ttl-ctrl") == nil
	}, 60*time.Second, time.Second,
		"control record (ttl=12s) must be removed by NSUP within ttl+nsup-period; check nsup-period 10 is set on namespace %s", eeAPNamespace)

	assert.NotNil(t, eeRead(t, client, eeAPNamespace, "ttl-never"),
		"record with ttl=never must still be present after NSUP ran")
}

// A durable delete leaves a tombstone behind so the record cannot be
// resurrected by a cold restart. Community Edition rejects the flag outright.
func TestEEDurableDelete(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, `
durable_delete: true
operation: '${! meta("op") }'
`)

	create := msg(t, `{"id":"d1","v":"here"}`)
	create.MetaSet("op", "write")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, eeRead(t, client, eeAPNamespace, "d1"))

	before := settledStat(t, client, eeAPNamespace, "tombstones")

	del := msg(t, `{"id":"d1"}`)
	del.MetaSet("op", "delete")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{del}))

	assert.Nil(t, eeRead(t, client, eeAPNamespace, "d1"), "record should be gone")
	assert.Greater(t, settledStat(t, client, eeAPNamespace, "tombstones"), before,
		"a durable delete must leave a tombstone, not expunge the record")
}

// Without durable_delete the delete expunges, leaving no tombstone. This is the
// control that proves the assertion above is actually measuring the flag.
func TestEENonDurableDeleteLeavesNoTombstone(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, `
durable_delete: false
operation: '${! meta("op") }'
`)

	create := msg(t, `{"id":"d2","v":"here"}`)
	create.MetaSet("op", "write")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))

	before := settledStat(t, client, eeAPNamespace, "tombstones")

	del := msg(t, `{"id":"d2"}`)
	del.MetaSet("op", "delete")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{del}))

	assert.Nil(t, eeRead(t, client, eeAPNamespace, "d2"))
	assert.Equal(t, before, settledStat(t, client, eeAPNamespace, "tombstones"),
		"an expunging delete must not create a tombstone")
}

func TestEECommitLevels(t *testing.T) {
	for _, level := range []string{"all", "master"} {
		t.Run(level, func(t *testing.T) {
			w, client := eeWriter(t, eeAPNamespace, "commit_level: "+level+"\n")

			require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
				msg(t, `{"id":"cl1","v":"`+level+`"}`),
			}))
			rec := eeRead(t, client, eeAPNamespace, "cl1")
			require.NotNil(t, rec)
			assert.Equal(t, level, rec.Bins["v"])
		})
	}
}

// Every replica policy has to return the record. With replication-factor 2 and
// two nodes, master_proles and random genuinely reach a non-master copy.
func TestEEReplicaPolicies(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"r1","v":"replicated"}`),
	}))
	require.NotNil(t, eeRead(t, client, eeAPNamespace, "r1"))

	for _, replica := range []string{"sequence", "master", "master_proles", "random"} {
		t.Run(replica, func(t *testing.T) {
			for _, mode := range []string{"one", "all"} {
				p := eeLookup(t, eeAPNamespace, `
replica: `+replica+`
read_mode_ap: `+mode+`
bins: [ "v" ]
`)
				out, err := p.ProcessBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"r1"}`)})
				require.NoError(t, err)
				require.Len(t, out, 1)
				require.Len(t, out[0], 1)
				require.NoError(t, out[0][0].GetError())
				assert.Equal(t, map[string]any{"v": "replicated"}, structured(t, out[0][0]),
					"replica=%v read_mode_ap=%v", replica, mode)
			}
		})
	}
}

// Strong consistency is the mode the connector documents as requiring
// commit_level: all, so both the write and every read mode must work against a
// namespace with strong-consistency enabled and a roster set.
func TestEEStrongConsistency(t *testing.T) {
	w, client := eeWriter(t, eeSCNamespace, "commit_level: all\n")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"sc1","v":"consistent"}`),
	}))
	require.NotNil(t, eeRead(t, client, eeSCNamespace, "sc1"))

	for _, mode := range []string{"session", "linearize", "allow_replica", "allow_unavailable"} {
		t.Run(mode, func(t *testing.T) {
			p := eeLookup(t, eeSCNamespace, `
read_mode_sc: `+mode+`
bins: [ "v" ]
`)
			out, err := p.ProcessBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"sc1"}`)})
			require.NoError(t, err)
			require.Len(t, out, 1)
			require.Len(t, out[0], 1)
			require.NoError(t, out[0][0].GetError())
			assert.Equal(t, map[string]any{"v": "consistent"}, structured(t, out[0][0]))
		})
	}
}

// A batch spanning both nodes must complete whether the client fans out across
// nodes or walks them one at a time.
func TestEEConcurrentNodes(t *testing.T) {
	for _, concurrency := range []string{"0", "1", "2"} {
		t.Run("concurrent_nodes_"+concurrency, func(t *testing.T) {
			w, client := eeWriter(t, eeAPNamespace, "concurrent_nodes: "+concurrency+"\n")

			var batch service.MessageBatch
			for i := range 200 {
				batch = append(batch, msg(t, `{"id":"cn`+strconv.Itoa(i)+`","v":`+strconv.Itoa(i)+`}`))
			}
			require.NoError(t, w.WriteBatch(t.Context(), batch))

			for _, i := range []int{0, 99, 199} {
				rec := eeRead(t, client, eeAPNamespace, "cn"+strconv.Itoa(i))
				require.NotNil(t, rec, "key cn%d missing", i)
				assert.Equal(t, i, rec.Bins["v"])
			}
		})
	}
}

// max_in_flight batches running at once must not outrun the connection pool.
// An empty pool is a hard failure rather than a wait in the Aerospike client,
// and writes default to max_retries: 0, so a pool that fills lazily loses every
// batch that arrives while it is still growing. This only shows up on a
// multi-node cluster, where a batch needs a connection per node at once.
func TestEEConcurrentBatchesDoNotStarveThePool(t *testing.T) {
	const concurrency = 64

	w, _ := eeWriter(t, eeAPNamespace, "max_in_flight: "+strconv.Itoa(concurrency)+"\n")

	var failed atomic.Int64
	var wg sync.WaitGroup
	for g := range concurrency {
		wg.Go(func() {
			var batch service.MessageBatch
			for i := range 20 {
				id := "starve" + strconv.Itoa(g) + "_" + strconv.Itoa(i)
				batch = append(batch, msg(t, `{"id":"`+id+`","v":`+strconv.Itoa(i)+`}`))
			}
			if err := w.WriteBatch(t.Context(), batch); err != nil {
				failed.Add(1)
			}
		})
	}
	wg.Wait()

	assert.Zero(t, failed.Load(), "%d of %d concurrent batches failed on a cold pool", failed.Load(), concurrency)
}

// The cluster name is a guard against pointing a pipeline at the wrong cluster,
// so a mismatch has to be refused rather than silently connecting.
func TestEEClusterNameMismatchIsRefused(t *testing.T) {
	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this'
cluster_name: definitely-not-the-cluster
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	err := w.Connect(t.Context())
	require.Error(t, err, "a wrong cluster_name must not connect")
}

// secHost addresses a cluster with access control enabled. Community Edition
// has no security subsystem at all, so this only runs against Enterprise.
func secHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("AEROSPIKE_SEC_HOST")
	if host == "" {
		t.Skip("AEROSPIKE_SEC_HOST is unset; access-control tests need a secured Enterprise node")
	}
	ensureSecUser(t)
	return host
}

// secUserOnce creates rpcn/rpcnpass once per test process. The account is
// stored in the node's smd directory, mounted at smd/ee-sec, so a container
// recreate keeps it. CreateUser runs against AEROSPIKE_SEC_HOST as admin,
// so the tests are not tied to the as-ee-sec container name.
var (
	secUserOnce sync.Once
	secUserErr  error
)

func ensureSecUser(t *testing.T) {
	t.Helper()
	secUserOnce.Do(func() {
		secUserErr = createSecUser()
	})
	require.NoError(t, secUserErr)
}

func createSecUser() error {
	raw := os.Getenv("AEROSPIKE_SEC_HOST")
	if raw == "" {
		return errors.New("AEROSPIKE_SEC_HOST is unset; creating user rpcn needs the secured node's plaintext address")
	}
	host, err := parseHost(raw)
	if err != nil {
		return fmt.Errorf("AEROSPIKE_SEC_HOST %q: %w", raw, err)
	}
	policy := as.NewClientPolicy()
	policy.User = "admin"
	policy.Password = "admin"
	policy.Timeout = 5 * time.Second
	policy.LoginTimeout = 5 * time.Second

	const user = "rpcn"
	roles := []string{"read-write", "sys-admin"}
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		client, connErr := as.NewClientWithPolicyAndHost(policy, host)
		if connErr != nil {
			last = connErr
			time.Sleep(time.Second)
			continue
		}
		createErr := client.CreateUser(nil, user, "rpcnpass", roles)
		client.Close()
		if createErr == nil || resultCodeIs(createErr, types.USER_ALREADY_EXISTS) {
			return nil
		}
		last = createErr
		time.Sleep(time.Second)
	}
	return fmt.Errorf("creating user %s on %s: %w", user, raw, last)
}

func resultCodeIs(err error, code types.ResultCode) bool {
	var ae as.Error
	return errors.As(err, &ae) && ae.Matches(code)
}

func secWriter(t *testing.T, extraYAML string) *aerospikeWriter {
	t.Helper()

	yaml := `
hosts: [ "` + secHost(t) + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func TestEEAuthInternal(t *testing.T) {
	w := secWriter(t, `
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
`)
	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"auth1","v":"authenticated"}`),
	}))
}

func TestEEAuthRejectsBadPassword(t *testing.T) {
	w := secWriter(t, `
auth_mode: internal
credentials:
  username: rpcn
  password: definitely-wrong
`)
	require.Error(t, w.Connect(t.Context()), "a wrong password must not connect")
}

func TestEEAuthRequiredWhenSecurityEnabled(t *testing.T) {
	w := secWriter(t, "")
	require.Error(t, w.Connect(t.Context()), "a secured cluster must refuse an unauthenticated client")
}

// resolveTLSCA returns an absolute path for the CA file named by ca.
// go test runs with this package as cwd, so a repo-root relative path is also
// tried with the internal/impl/aerospike prefix removed. That is the same
// file. A missing path is an error; another certificate is not substituted.
func resolveTLSCA(t *testing.T, ca string) string {
	t.Helper()
	candidates := []string{ca}
	if !filepath.IsAbs(ca) {
		if trimmed, ok := strings.CutPrefix(strings.TrimPrefix(ca, "./"), "internal/impl/aerospike/"); ok && trimmed != "" {
			candidates = append(candidates, trimmed)
		}
	}
	for _, p := range candidates {
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			continue
		}
		abs, err := filepath.Abs(p)
		require.NoError(t, err)
		return abs
	}
	t.Fatalf("AEROSPIKE_TLS_CA %q not found", ca)
	return ""
}

// tlsMaterial is the secured Enterprise node. The TLS port requires a client
// certificate (tls-authenticate-client any). client.pem is CN rpcn.
// client.enc.key is that key encrypted with password rpcnkey.
func tlsMaterial(t *testing.T) (host, ca, cert, key, encKey string) {
	t.Helper()
	host = os.Getenv("AEROSPIKE_TLS_HOST")
	caEnv := os.Getenv("AEROSPIKE_TLS_CA")
	if host == "" || caEnv == "" {
		t.Skip("AEROSPIKE_TLS_HOST/AEROSPIKE_TLS_CA unset; TLS tests need a TLS-enabled Enterprise node")
	}
	ca = resolveTLSCA(t, caEnv)
	dir := filepath.Dir(ca)
	cert = filepath.Join(dir, "client.pem")
	key = filepath.Join(dir, "client.key")
	encKey = filepath.Join(dir, "client.enc.key")
	require.FileExists(t, cert, "run testdata/ee/gen-certs.sh; the TLS port requires a client certificate")
	require.FileExists(t, key)
	require.FileExists(t, encKey)
	ensureSecUser(t)
	return host, ca, cert, key, encKey
}

func tlsClientYAML(ca, cert, key, keyPassword string) string {
	passwordLine := ""
	if keyPassword != "" {
		passwordLine = "\n      password: " + keyPassword
	}
	return `
tls:
  enabled: true
  root_cas_file: ` + ca + `
  client_certs:
    - cert_file: ` + cert + `
      key_file: ` + key + passwordLine + `
`
}

// TLS needs the host spec to carry the server's tls-name, which is the
// host:tlsname:port form the connector documents. Login is still the
// internal password. The client certificate satisfies mutual TLS.
func TestEETLS(t *testing.T) {
	host, ca, cert, key, _ := tlsMaterial(t)

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
` + tlsClientYAML(ca, cert, key, "")
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"tls1","v":"encrypted"}`),
	}))
}

// The TLS listener requires a client certificate. A trusted CA and a
// password are not enough.
func TestEETLSRejectsMissingClientCert(t *testing.T) {
	host, ca, _, _, _ := tlsMaterial(t)

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
tls:
  enabled: true
  root_cas_file: ` + ca + `
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	require.Error(t, w.Connect(t.Context()), "a connection with no client certificate must be refused")
}

// client.enc.key is client.key wrapped as encrypted PKCS#8. The password
// decrypts it before the handshake. The server still sees the rpcn certificate.
func TestEETLSEncryptedKey(t *testing.T) {
	host, ca, cert, _, encKey := tlsMaterial(t)

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
` + tlsClientYAML(ca, cert, encKey, "rpcnkey")
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"tls-enc","v":"encrypted-key"}`),
	}))
}

// TestEETLSCertRefreshInsertsOnNewConnection replaces the client certificate
// files while the output stays connected. The poller (cert_refresh_interval
// 10s) loads the new files. The pooled connection, which still has the old
// handshake, is discarded, and the next write opens a connection with the new
// certificate and inserts a record. The output is not closed or connected again.
func TestEETLSCertRefreshInsertsOnNewConnection(t *testing.T) {
	host, caPath, _, _, _ := tlsMaterial(t)
	caPEM, err := os.ReadFile(caPath)
	require.NoError(t, err)
	caKeyPEM, err := os.ReadFile(filepath.Join(filepath.Dir(caPath), "ca.key"))
	require.NoError(t, err)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	origCert, origKey := signEEClientCert(t, caPEM, caKeyPEM, 1)
	require.NoError(t, os.WriteFile(certPath, origCert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, origKey, 0o600))

	w := newTestWriter(t, `
hosts: [ "`+host+`" ]
namespace: `+eeAPNamespace+`
set: `+eeSet+`
key: '${! json("id") }'
bins: 'root = this.without("id")'
min_connections_per_node: 1
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
tls:
  enabled: true
  root_cas_file: `+caPath+`
  client_certs:
    - cert_file: `+certPath+`
      key_file: `+keyPath+`
cert_refresh_interval: 10s
`)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	require.NoError(t, w.Connect(t.Context()))
	requireWrite(t, w, `{"id":"tls-refresh-old","v":"before"}`)
	oldSerial := presentedSerial(t, w)

	nextCert, nextKey := signEEClientCert(t, caPEM, caKeyPEM, 2)
	require.NoError(t, os.WriteFile(certPath, nextCert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, nextKey, 0o600))

	require.Eventually(t, func() bool {
		return presentedSerial(t, w).Cmp(oldSerial) != 0
	}, 25*time.Second, 200*time.Millisecond, "poller did not load the replacement client certificate")
	assert.Equal(t, 0, presentedSerial(t, w).Cmp(big.NewInt(2)))

	client, release := w.conn.acquire()
	t.Cleanup(release)
	require.NotNil(t, client, "the output must stay connected across the refresh")
	discardPooledConnections(t, client, w.conf.client.Policy.ConnectionQueueSize)

	requireWrite(t, w, `{"id":"tls-refresh-new","v":"after"}`)

	reader, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(reader.Close)
	rec := eeRead(t, reader, eeAPNamespace, "tls-refresh-new")
	require.NotNil(t, rec)
	assert.Equal(t, "after", rec.Bins["v"])
}

func requireWrite(t *testing.T, w *aerospikeWriter, body string) {
	t.Helper()
	batch := service.MessageBatch{msg(t, body)}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	if err == nil {
		return
	}
	require.NoError(t, firstIndexedError(t, indexer, err))
}

func presentedSerial(t *testing.T, w *aerospikeWriter) *big.Int {
	t.Helper()
	got, err := w.conf.client.Policy.TlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.NotEmpty(t, got.Certificate)
	leaf, err := x509.ParseCertificate(got.Certificate[0])
	require.NoError(t, err)
	return leaf.SerialNumber
}

// discardPooledConnections closes connections that finished their handshake
// before the certificate reload. The client then opens a replacement, and that
// handshake presents whatever GetClientCertificate returns now.
//
// GetConnection opens a new connection when the pool is empty, so draining
// until it fails never ends on a host where the handshake is fast. A pool holds
// at most poolSize connections, so taking that many removes every old one.
func discardPooledConnections(t *testing.T, client *as.Client, poolSize int) {
	t.Helper()
	for _, node := range client.GetNodes() {
		for range poolSize {
			conn, err := node.GetConnection(20 * time.Millisecond)
			if err != nil || conn == nil {
				break
			}
			node.InvalidateConnection(conn)
		}
		require.Eventually(t, func() bool {
			conn, err := node.GetConnection(200 * time.Millisecond)
			if err != nil || conn == nil {
				return false
			}
			node.PutConnection(conn)
			return true
		}, 15*time.Second, 100*time.Millisecond, "node %s did not open a connection after the certificate refresh", node.GetName())
	}
}

func signEEClientCert(t *testing.T, caPEM, caKeyPEM []byte, serial int64) (certPEM, keyPEM []byte) {
	t.Helper()
	ca, caKey := parseEECA(t, caPEM, caKeyPEM)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "rpcn"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
}

func parseEECA(t *testing.T, caPEM, caKeyPEM []byte) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	block, _ := pem.Decode(caPEM)
	require.NotNil(t, block, "ca.pem is not PEM")
	ca, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	keyBlock, _ := pem.Decode(caKeyPEM)
	require.NotNil(t, keyBlock, "ca.key is not PEM")
	if key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes); err == nil {
		return ca, key
	}
	key, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	require.NoError(t, err)
	rsaKey, ok := key.(*rsa.PrivateKey)
	require.True(t, ok, "ca.key is not an RSA private key")
	return ca, rsaKey
}

// PKI login sends no password. The certificate CN is the username rpcn.
// This tools image cannot create a PKI-only user; show users lists rpcn
// as password,PKI, so the same client certificate is used.
func TestEEAuthPKI(t *testing.T) {
	host, ca, cert, key, _ := tlsMaterial(t)

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: pki
` + tlsClientYAML(ca, cert, key, "")
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"pki1","v":"cert"}`),
	}))

	client, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	rec := eeRead(t, client, eeAPNamespace, "pki1")
	require.NotNil(t, rec)
	assert.Equal(t, "cert", rec.Bins["v"])
}

// This cluster has no LDAP service. External login must reach the server
// over TLS and be refused, rather than fall back to the internal password.
func TestEEAuthExternalRejected(t *testing.T) {
	host, ca, cert, key, _ := tlsMaterial(t)

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
connect_timeout: 5s
auth_mode: external
credentials:
  username: ldap-user
  password: secret
` + tlsClientYAML(ca, cert, key, "")
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	err := w.Connect(ctx)
	require.Error(t, err)
	// The server has no LDAP service. It returns result code 90, which this
	// client build does not name. The client then wraps that as
	// INVALID_NODE_ERROR (-3), and AerospikeError.As returns the outer error,
	// so the code has to be matched down the chain. A missing TLS config
	// fails earlier and does not reach the server.
	var asErr as.Error
	require.ErrorAs(t, err, &asErr)
	assert.True(t, asErr.Matches(types.ResultCode(90)))
	assert.NotContains(t, err.Error(), "External Authentication requires TLS")
}

// An untrusted CA has to fail the handshake rather than fall back to plaintext
// or skip verification silently.
func TestEETLSRejectsUntrustedServer(t *testing.T) {
	host := os.Getenv("AEROSPIKE_TLS_HOST")
	if host == "" {
		t.Skip("AEROSPIKE_TLS_HOST unset")
	}

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
tls:
  enabled: true
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	err := w.Connect(t.Context())
	require.Error(t, err)
	// A trusted CA with no client certificate fails the handshake too
	// ("remote error: tls: handshake failure"). Only a verification failure
	// shows that the unknown CA was refused.
	assert.Contains(t, err.Error(), "tls: failed to verify certificate")
}
