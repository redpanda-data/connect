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
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const (
	aerospikeImage         = "aerospike/aerospike-server:8.2"
	integrationNamespace   = "test"
	integrationOutputSet   = "rpa_e2e"
	integrationDigestSet   = "rpa_digest"
	aerospikeContainerPort = "3000/tcp"
)

var (
	startOnce       sync.Once
	integrationAddr string
	startErr        error
)

func integrationHost(t *testing.T) string {
	t.Helper()
	integration.CheckSkip(t)
	startOnce.Do(func() {
		integrationAddr, startErr = startAerospike()
	})
	require.NoError(t, startErr)
	return integrationAddr
}

// startAerospike launches a single-node community server with nsup-period set
// so TTL writes are accepted.
//
// The client must use a host-mapped loopback address, not the container IP.
// Aerospike tells the client which address to use after the seed handshake;
// advertising the docker-bridge IP works on Linux but is unreachable from
// Docker Desktop on macOS. We pin a host port, set access-address/access-port
// to 127.0.0.1:<that port>, and connect there so CI Ubuntu and local Docker
// Desktop take the same path.
func startAerospike() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not locate testdata/aerospike.conf")
	}
	cfgPath := filepath.Join(filepath.Dir(thisFile), "testdata", "aerospike.conf")
	baseConf, err := os.ReadFile(cfgPath)
	if err != nil {
		return "", err
	}

	hostPort, err := freeHostPort()
	if err != nil {
		return "", err
	}
	cfg, err := aerospikeConfWithHostAccess(baseConf, hostPort)
	if err != nil {
		return "", err
	}

	// Not t.Context(): the container is shared and outlives any one test.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	hostPortStr := strconv.Itoa(hostPort)
	ctr, err := testcontainers.Run(ctx, aerospikeImage,
		testcontainers.WithExposedPorts(aerospikeContainerPort),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = mobynet.PortMap{
				mobynet.MustParsePort(aerospikeContainerPort): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPortStr}},
			}
		}),
		testcontainers.WithCmd("--config-file", "/opt/aerospike/etc/aerospike.conf"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            bytes.NewReader(cfg),
			ContainerFilePath: "/opt/aerospike/etc/aerospike.conf",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("soon there will be cake").WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		return "", err
	}

	mapped, err := ctr.MappedPort(ctx, aerospikeContainerPort)
	if err != nil {
		return "", err
	}
	if mapped.Port() != hostPortStr {
		return "", fmt.Errorf("aerospike host port: mapped %s, want %s", mapped.Port(), hostPortStr)
	}

	addr := net.JoinHostPort("127.0.0.1", hostPortStr)
	if err := waitForClient(ctx, addr); err != nil {
		return "", err
	}
	return addr, nil
}

func freeHostPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// servicePortLine matches the network.service port line. Heartbeat and fabric
// use other ports, so port 3000 is unique in the config.
var servicePortLine = regexp.MustCompile(`(?m)^([ \t]*)port[ \t]+3000[ \t]*\r?$`)

func aerospikeConfWithHostAccess(base []byte, hostPort int) ([]byte, error) {
	loc := servicePortLine.FindSubmatchIndex(base)
	if loc == nil {
		return nil, errors.New("testdata/aerospike.conf: missing 'port 3000' in network.service")
	}
	indent := string(base[loc[2]:loc[3]])
	insert := fmt.Sprintf("\n%saccess-address 127.0.0.1\n%saccess-port %d", indent, indent, hostPort)

	out := make([]byte, 0, len(base)+len(insert))
	out = append(out, base[:loc[1]]...)
	out = append(out, insert...)
	return append(out, base[loc[1]:]...), nil
}

func TestAerospikeConfWithHostAccess(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	base, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "testdata", "aerospike.conf"))
	require.NoError(t, err)

	got, err := aerospikeConfWithHostAccess(base, 18412)
	require.NoError(t, err)
	assert.Contains(t, string(got), "        port 3000\n        access-address 127.0.0.1\n        access-port 18412\n")
	assert.Equal(t, 1, strings.Count(string(got), "access-address"))

	got, err = aerospikeConfWithHostAccess([]byte("network {\n\tservice {\n\t\tport 3000\n\t}\n}\n"), 1)
	require.NoError(t, err)
	assert.Contains(t, string(got), "\t\tport 3000\n\t\taccess-address 127.0.0.1\n\t\taccess-port 1\n")

	_, err = aerospikeConfWithHostAccess([]byte("network {}\n"), 1)
	require.Error(t, err)
}

func waitForClient(ctx context.Context, addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		client, err := as.NewClient(host, port)
		if err == nil {
			client.Close()
			return nil
		}
		last = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("aerospike never accepted a client at %s: %w", addr, last)
}

func outputSetup(t *testing.T, extraYAML string) (*aerospikeWriter, *as.Client) {
	t.Helper()
	hosts := integrationHost(t)

	yaml := `
hosts: [ "` + hosts + `" ]
namespace: ` + integrationNamespace + `
set: ` + integrationOutputSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	client, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Truncate(nil, integrationNamespace, integrationOutputSet, nil))
	return w, client
}

func outputRead(t *testing.T, client *as.Client, id string) *as.Record {
	t.Helper()
	key, err := as.NewKey(integrationNamespace, integrationOutputSet, id)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	if asErr != nil && asErr.Matches(2 /* KEY_NOT_FOUND_ERROR */) {
		return nil
	}
	require.NoError(t, asErr)
	return rec
}

func msg(t *testing.T, body string) *service.Message {
	t.Helper()
	return service.NewMessage([]byte(body))
}

func TestIntegrationWriteAndRead(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"a1","name":"Ada","score":10,"ratio":0.5,"tags":["x","y"]}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "a1")
	require.NotNil(t, rec)
	assert.Equal(t, "Ada", rec.Bins["name"])
	// An integral JSON number must land as an Aerospike integer, not a double.
	assert.Equal(t, 10, rec.Bins["score"])
	assert.Equal(t, 0.5, rec.Bins["ratio"])
	assert.Equal(t, []any{"x", "y"}, rec.Bins["tags"])
	// The key field addresses the record and must not be stored again as a bin.
	assert.NotContains(t, rec.Bins, "id")
	assert.Len(t, rec.Bins, 4)
}

// digestTestWriters returns a writer addressing records by digest and one
// addressing them by user key, both on the digest test set with send_key on.
func digestTestWriters(t *testing.T) (byDigest, byKey *aerospikeWriter, client *as.Client) {
	t.Helper()
	hosts := integrationHost(t)
	common := `
hosts: [ "` + hosts + `" ]
namespace: ` + integrationNamespace + `
set: ` + integrationDigestSet + `
send_key: true
`
	byDigest = newTestWriter(t, common+`
digest: '${! json("digest") }'
bins: 'root = this.without("digest")'
`)
	byKey = newTestWriter(t, common+`
key: '${! json("id") }'
bins: 'root = this.without("id")'
`)
	for _, w := range []*aerospikeWriter{byDigest, byKey} {
		require.NoError(t, w.Connect(t.Context()))
		t.Cleanup(func() { _ = w.Close(context.Background()) })
	}

	client, err := as.NewClientWithPolicyAndHost(byDigest.conf.client.Policy, byDigest.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return byDigest, byKey, client
}

// storedUserKeys scans the digest test set and returns the user key the server
// stored for each of the given digests, or nil when none is stored. Get cannot
// show this, because it returns the key it was called with.
func storedUserKeys(t *testing.T, client *as.Client, digests ...[]byte) map[string]any {
	t.Helper()
	want := map[string]bool{}
	for _, d := range digests {
		want[string(d)] = true
	}
	rs, asErr := client.ScanAll(nil, integrationNamespace, integrationDigestSet)
	require.NoError(t, asErr)
	stored := map[string]any{}
	for res := range rs.Results() {
		require.NoError(t, res.Err)
		d := string(res.Record.Key.Digest())
		if !want[d] {
			continue
		}
		var userKey any
		if v := res.Record.Key.Value(); v != nil {
			userKey = v.GetObject()
		}
		stored[d] = userKey
	}
	return stored
}

// TestIntegrationWriteByDigest writes a record whose address is a producer-supplied
// digest. Reading it back with that digest finds the bins; addressing the Base64
// text as a user key does not, because that would hash the text instead of using
// the digest. With send_key on, the server stores no user key for the digest
// record, while a record written by key in the same set has one.
func TestIntegrationWriteByDigest(t *testing.T) {
	byDigest, byKey, client := digestTestWriters(t)
	raw := bytes.Repeat([]byte{0x11}, digestLen)
	b64 := base64.StdEncoding.EncodeToString(raw)

	require.NoError(t, byDigest.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"digest":"`+b64+`","name":"Ada"}`),
	}))
	require.NoError(t, byKey.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"grace","name":"Grace"}`),
	}))

	key, err := as.NewKeyWithDigest(integrationNamespace, integrationDigestSet, nil, raw)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	require.NoError(t, asErr)
	assert.Equal(t, "Ada", rec.Bins["name"])
	assert.NotContains(t, rec.Bins, "digest")

	hashed, err := as.NewKey(integrationNamespace, integrationDigestSet, b64)
	require.NoError(t, err)
	_, asErr = client.Get(nil, hashed)
	require.Error(t, asErr)
	assert.True(t, asErr.Matches(types.KEY_NOT_FOUND_ERROR))

	grace, err := as.NewKey(integrationNamespace, integrationDigestSet, "grace")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		string(raw):            nil,
		string(grace.Digest()): "grace",
	}, storedUserKeys(t, client, raw, grace.Digest()))
}

// TestIntegrationUpdateByKeyDigest writes a record by user key, then updates it
// by that key's digest. The update changes the bins and keeps the user key the
// first write stored, because a write by digest sends no user key.
func TestIntegrationUpdateByKeyDigest(t *testing.T) {
	byDigest, byKey, client := digestTestWriters(t)

	require.NoError(t, byKey.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ingest-1","name":"Ada","city":"London"}`),
	}))

	key, err := as.NewKey(integrationNamespace, integrationDigestSet, "ingest-1")
	require.NoError(t, err)
	b64 := base64.StdEncoding.EncodeToString(key.Digest())
	require.NoError(t, byDigest.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"digest":"`+b64+`","name":"Grace"}`),
	}))

	rec, asErr := client.Get(nil, key)
	require.NoError(t, asErr)
	assert.Equal(t, "Grace", rec.Bins["name"])
	assert.Equal(t, "London", rec.Bins["city"])
	assert.Equal(t, map[string]any{string(key.Digest()): "ingest-1"}, storedUserKeys(t, client, key.Digest()))
}

// TestIntegrationTopicRoutesToSet writes one batch to two sets under one user
// key. The set name is the Kafka topic, which is how a fixed topic list shares
// one namespace. The coalescing key includes the set, so the two messages stay
// two records.
func TestIntegrationTopicRoutesToSet(t *testing.T) {
	w, client := outputSetup(t, `
set: '${! meta("kafka_topic") }'
`)
	for _, set := range []string{"route_users", "route_orders"} {
		require.NoError(t, client.Truncate(nil, integrationNamespace, set, nil))
	}

	const id = "u-42"
	users := msg(t, `{"id":"`+id+`","email":"a@b.com"}`)
	users.MetaSet("kafka_topic", "route_users")
	orders := msg(t, `{"id":"`+id+`","sku":"book"}`)
	orders.MetaSet("kafka_topic", "route_orders")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{users, orders}))

	read := func(set string) *as.Record {
		t.Helper()
		key, err := as.NewKey(integrationNamespace, set, id)
		require.NoError(t, err)
		rec, asErr := client.Get(nil, key)
		require.NoError(t, asErr)
		return rec
	}

	usersRec := read("route_users")
	ordersRec := read("route_orders")
	assert.Equal(t, "a@b.com", usersRec.Bins["email"])
	assert.NotContains(t, usersRec.Bins, "sku")
	assert.Equal(t, "book", ordersRec.Bins["sku"])
	assert.NotContains(t, ordersRec.Bins, "email")
	assert.Nil(t, outputRead(t, client, id), "the default set must not receive a routed write")
}

// TestIntegrationJSONFieldRoutesNamespaceAndSet writes one batch to two
// declared namespaces. The same id and set stay two records because the
// coalescing key includes the namespace.
func TestIntegrationJSONFieldRoutesNamespaceAndSet(t *testing.T) {
	w, client := outputSetup(t, `
namespace: '${! json("namespace_name") }'
set: '${! json("set_name") }'
bins: 'root = this.without("id", "namespace_name", "set_name")'
`)
	const set = "route_clicks"
	const id = "e-1"
	for _, namespace := range []string{integrationNamespace, noNSUPNamespace} {
		require.NoError(t, client.Truncate(nil, namespace, set, nil))
	}

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"`+id+`","namespace_name":"`+integrationNamespace+`","set_name":"`+set+`","n":1}`),
		msg(t, `{"id":"`+id+`","namespace_name":"`+noNSUPNamespace+`","set_name":"`+set+`","n":2}`),
	}))

	read := func(namespace string) *as.Record {
		t.Helper()
		key, err := as.NewKey(namespace, set, id)
		require.NoError(t, err)
		rec, asErr := client.Get(nil, key)
		require.NoError(t, asErr)
		return rec
	}

	testRec := read(integrationNamespace)
	nosupRec := read(noNSUPNamespace)
	assert.Equal(t, 1, testRec.Bins["n"])
	assert.Equal(t, 2, nosupRec.Bins["n"])
	assert.NotContains(t, testRec.Bins, "namespace_name")
	assert.NotContains(t, testRec.Bins, "set_name")
	assert.NotContains(t, nosupRec.Bins, "namespace_name")
	assert.NotContains(t, nosupRec.Bins, "set_name")
	assert.Nil(t, outputRead(t, client, id), "the default set must not receive a routed write")
}

// TestIntegrationInvalidNamespaceFailsOneMessage writes one batch at a declared
// namespace and a name that is not declared. Code 20 fails only the bad
// message. The good record is stored. With auto_replay_nacks on, the failed
// message is replayed forever, which is why the routing example wraps that
// branch in output.fallback.
func TestIntegrationInvalidNamespaceFailsOneMessage(t *testing.T) {
	w, client := outputSetup(t, `
namespace: '${! json("namespace_name") }'
`)

	batch := service.MessageBatch{
		msg(t, `{"id":"ns20-bad","namespace_name":"not_a_namespace","v":1}`),
		msg(t, `{"id":"ns20-ok","namespace_name":"`+integrationNamespace+`","v":1}`),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())
	msgErr := firstIndexedError(t, indexer, err)
	assert.Contains(t, msgErr.Error(), "does not exist")
	assert.Contains(t, msgErr.Error(), "code 20")
	assert.NotNil(t, outputRead(t, client, "ns20-ok"))
	assert.Nil(t, outputRead(t, client, "ns20-bad"))
}

// TestIntegrationCoalescing proves the merge rules against a real server: three
// messages for one key inside one batch produce one record with the merged bins
// and the last writer winning, rather than three contending commands.
func TestIntegrationCoalescing(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"c1","a":1,"shared":"first"}`),
		msg(t, `{"id":"c2","a":1}`),
		msg(t, `{"id":"c1","b":2}`),
		msg(t, `{"id":"c1","shared":"last"}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "c1")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"])
	assert.Equal(t, "last", rec.Bins["shared"])

	assert.NotNil(t, outputRead(t, client, "c2"))
}

// TestIntegrationCoalesceDisjointBins proves two messages for one key in the
// same batch, with no bin names in common, become one record that has both
// sets of bins.
func TestIntegrationCoalesceDisjointBins(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"c3","a":1,"b":2}`),
		msg(t, `{"id":"c3","c":3,"d":4}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "c3")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"])
	assert.Equal(t, 3, rec.Bins["c"])
	assert.Equal(t, 4, rec.Bins["d"])
	assert.NotContains(t, rec.Bins, "id")
}

// TestIntegrationBinNamesAreCaseSensitive proves a bin name keeps its case.
// "tier" and "Tier" are two bins, and the same spelling difference inside a
// map bin is two map keys.
func TestIntegrationBinNamesAreCaseSensitive(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"case1","tier":"gold","Tier":"silver","prefs":{"theme":"dark","Theme":"light"}}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "case1")
	require.NotNil(t, rec)
	assertNestedBins(t, rec.Bins, map[string]any{
		"tier":  "gold",
		"Tier":  "silver",
		"prefs": map[string]any{"theme": "dark", "Theme": "light"},
	})
}

// TestIntegrationSameKeyAcrossBatches locks the boundary between batches.
// Messages in different WriteBatch calls are not folded. A later write keeps
// bins from the earlier batch. A later delete removes the record. The same
// write-then-delete pair inside one batch must not leave the record, because
// that pair is folded into a single delete.
func TestIntegrationSameKeyAcrossBatches(t *testing.T) {
	w, client := outputSetup(t, "operation: '${! meta(\"op\") }'\n")

	t.Run("later write keeps bins from the earlier batch", func(t *testing.T) {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"x1","a":1,"b":2}`),
		}))
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"x1","c":3,"d":4}`),
		}))

		rec := outputRead(t, client, "x1")
		require.NotNil(t, rec)
		assert.Equal(t, 1, rec.Bins["a"])
		assert.Equal(t, 2, rec.Bins["b"])
		assert.Equal(t, 3, rec.Bins["c"])
		assert.Equal(t, 4, rec.Bins["d"])
	})

	t.Run("later delete removes the record", func(t *testing.T) {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"x2","a":1,"b":2}`),
		}))
		require.NotNil(t, outputRead(t, client, "x2"))

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "delete", `{"id":"x2"}`),
		}))
		assert.Nil(t, outputRead(t, client, "x2"))
	})

	t.Run("write then delete in one batch leaves nothing", func(t *testing.T) {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"x3","a":1,"b":2}`),
			opMsg(t, "delete", `{"id":"x3"}`),
		}))
		assert.Nil(t, outputRead(t, client, "x3"))
	})
}

func opMsg(t *testing.T, op, body string) *service.Message {
	t.Helper()
	m := msg(t, body)
	m.MetaSet("op", op)
	return m
}

func TestIntegrationTombstoneDeletes(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
`)

	create := msg(t, `{"v":1}`)
	create.MetaSet("k", "d1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, outputRead(t, client, "d1"))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "d1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))
	assert.Nil(t, outputRead(t, client, "d1"))

	// Deleting an already absent record must stay a success, otherwise a
	// redelivered tombstone would nack forever.
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))
}

func TestIntegrationTombstoneDisabledPreservesRecord(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
tombstone_as_delete: false
bins: 'root = if content().length() == 0 { {"touch": true} } else { this }'
`)

	create := msg(t, `{"v":1}`)
	create.MetaSet("k", "td1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, outputRead(t, client, "td1"))

	empty := msg(t, ``)
	empty.MetaSet("k", "td1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{empty}))

	rec := outputRead(t, client, "td1")
	require.NotNil(t, rec, "an empty payload must not delete when tombstone handling is off")
	assert.Equal(t, 1, rec.Bins["v"])
	assert.Equal(t, true, rec.Bins["touch"])
}

func TestIntegrationFencing(t *testing.T) {
	w, client := outputSetup(t, `
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	newer := msg(t, `{"id":"f1","v":"second"}`)
	newer.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newer}))
	assert.Equal(t, "second", outputRead(t, client, "f1").Bins["v"])

	stale := msg(t, `{"id":"f1","v":"first"}`)
	stale.MetaSet("off", "3")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{stale}))

	rec := outputRead(t, client, "f1")
	assert.Equal(t, "second", rec.Bins["v"], "a stale replay must not overwrite newer data")
	assert.Equal(t, 10, rec.Bins["_off"])

	newest := msg(t, `{"id":"f1","v":"third"}`)
	newest.MetaSet("off", "11")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newest}))
	assert.Equal(t, "third", outputRead(t, client, "f1").Bins["v"])
}

func TestIntegrationFencingDeleteThenStaleWrite(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	create := msg(t, `{"v":"live"}`)
	create.MetaSet("k", "fd1")
	create.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, outputRead(t, client, "fd1"))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "fd1")
	tombstone.MetaSet("off", "11")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))

	rec := outputRead(t, client, "fd1")
	require.NotNil(t, rec, "a fenced delete must leave a tombstone record")
	assert.Equal(t, true, rec.Bins["_deleted"])
	assert.Equal(t, 11, rec.Bins["_off"])

	stale := msg(t, `{"v":"resurrect"}`)
	stale.MetaSet("k", "fd1")
	stale.MetaSet("off", "3")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{stale}))

	rec = outputRead(t, client, "fd1")
	require.NotNil(t, rec)
	assert.Equal(t, true, rec.Bins["_deleted"], "a stale write must not resurrect a fenced delete")
	assert.NotContains(t, rec.Bins, "v")

	newer := msg(t, `{"v":"again"}`)
	newer.MetaSet("k", "fd1")
	newer.MetaSet("off", "12")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newer}))
	rec = outputRead(t, client, "fd1")
	require.NotNil(t, rec)
	assert.Equal(t, "again", rec.Bins["v"])
	assert.NotContains(t, rec.Bins, "_deleted")
}

func TestIntegrationTTL(t *testing.T) {
	w, client := outputSetup(t, "ttl: 1h\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"t1","v":1}`)}))

	rec := outputRead(t, client, "t1")
	require.NotNil(t, rec)
	// Requires nsup-period > 0 on the namespace; with NSUP disabled the write
	// would have been rejected outright.
	assert.InDelta(t, 3600, rec.Expiration, 60)
}

func TestIntegrationTTLFromJSON(t *testing.T) {
	w, client := outputSetup(t, "ttl: '${! json(\"ttl\") }'\n")

	for _, tc := range []struct {
		id   string
		body string
		want float64
	}{
		{id: "t24h", body: `{"id":"t24h","v":1,"ttl":"24H"}`, want: 86400},
		{id: "t1d", body: `{"id":"t1d","v":1,"ttl":"1D"}`, want: 86400},
		{id: "t3600", body: `{"id":"t3600","v":1,"ttl":3600}`, want: 3600},
	} {
		t.Run(tc.id, func(t *testing.T) {
			require.NoError(t, w.WriteBatch(t.Context(),
				service.MessageBatch{msg(t, tc.body)}))
			rec := outputRead(t, client, tc.id)
			require.NotNil(t, rec)
			assert.InDelta(t, tc.want, rec.Expiration, 60)
		})
	}
}

// TestIntegrationTTLKeepPreservesVoidTime updates a record with ttl keep.
// The void-time from the first write must stay; a fresh TTL would replace it.
func TestIntegrationTTLKeepPreservesVoidTime(t *testing.T) {
	w, client := outputSetup(t, "ttl: '${! json(\"ttl\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"tk1","v":1,"ttl":"24H"}`)}))
	first := outputRead(t, client, "tk1")
	require.NotNil(t, first)
	assert.InDelta(t, 86400, first.Expiration, 60)

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"tk1","v":2,"ttl":"keep"}`)}))
	second := outputRead(t, client, "tk1")
	require.NotNil(t, second)
	assert.Equal(t, 2, second.Bins["v"])
	assert.InDelta(t, first.Expiration, second.Expiration, 5)
}

func TestIntegrationTTLRejectsInvalidJSON(t *testing.T) {
	w, _ := outputSetup(t, "ttl: '${! json(\"ttl\") }'\n")

	batch := service.MessageBatch{msg(t, `{"id":"tbad","v":1,"ttl":"24X"}`)}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "ttl")
}

func TestIntegrationReplaceClearsOldBins(t *testing.T) {
	w, client := outputSetup(t, "operation: replace\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"r1","a":1,"b":2}`)}))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"r1","a":9}`)}))

	rec := outputRead(t, client, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, 9, rec.Bins["a"])
	assert.NotContains(t, rec.Bins, "b", "replace must drop bins not named in the write")
}

// TestIntegrationWriteKeepsExistingBins proves write merges across batches:
// a later message adds a bin and leaves bins from the earlier write in place.
func TestIntegrationWriteKeepsExistingBins(t *testing.T) {
	w, client := outputSetup(t, "operation: write\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"m1","a":1}`)}))
	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"m1","b":2}`)}))

	rec := outputRead(t, client, "m1")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"])
}

// TestIntegrationWriteAndUpdateNestedJSON stores lists, maps, and nested
// collections as bins, then writes the same key again. A later write merges
// whole bins: bins absent from the second message stay, including their nested
// values. operation update is an alias of write, so both messages use write.
func TestIntegrationWriteAndUpdateNestedJSON(t *testing.T) {
	const created = `{
		"id": "user-1001",
		"name": "SomeName",
		"addresses": [
			{"type": "home", "city": "Bangalore", "state": "Karnataka", "zip": 560001, "location": {"lat": 12.9716, "lon": 77.5946}},
			{"type": "office", "city": "Chennai", "state": "Tamil Nadu", "zip": 600001, "location": {"lat": 13.0827, "lon": 80.2707}}
		],
		"single_map": {"name": "primary", "value": "test-value"},
		"simple_list": ["red", "green", "blue"],
		"list_of_lists": [[1, 2, 3], [10, 20, 30], [100, 200, 300]],
		"map_of_maps": {
			"personal": {"email": "test@example.com", "phone": "9999999999"},
			"work": {"company": "Aerospike", "role": "Engineer"}
		},
		"complex_map": {
			"profile": {"first_name": "SomeName", "last_name": "M"},
			"skills": ["Aerospike", "Kafka", "Java"],
			"projects": [
				{"name": "project-a", "status": "active", "technologies": ["Kafka", "Aerospike"]},
				{"name": "project-b", "status": "completed", "technologies": ["Python", "Docker"]}
			]
		}
	}`

	w, client := outputSetup(t, "operation: write\n")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, created)}))

	rec := outputRead(t, client, "user-1001")
	require.NotNil(t, rec)
	assert.NotContains(t, rec.Bins, "id")
	assertNestedBins(t, rec.Bins, map[string]any{
		"name": "SomeName",
		"addresses": []any{
			map[string]any{"type": "home", "city": "Bangalore", "state": "Karnataka", "zip": 560001, "location": map[string]any{"lat": 12.9716, "lon": 77.5946}},
			map[string]any{"type": "office", "city": "Chennai", "state": "Tamil Nadu", "zip": 600001, "location": map[string]any{"lat": 13.0827, "lon": 80.2707}},
		},
		"single_map":    map[string]any{"name": "primary", "value": "test-value"},
		"simple_list":   []any{"red", "green", "blue"},
		"list_of_lists": []any{[]any{1, 2, 3}, []any{10, 20, 30}, []any{100, 200, 300}},
		"map_of_maps": map[string]any{
			"personal": map[string]any{"email": "test@example.com", "phone": "9999999999"},
			"work":     map[string]any{"company": "Aerospike", "role": "Engineer"},
		},
		"complex_map": map[string]any{
			"profile": map[string]any{"first_name": "SomeName", "last_name": "M"},
			"skills":  []any{"Aerospike", "Kafka", "Java"},
			"projects": []any{
				map[string]any{"name": "project-a", "status": "active", "technologies": []any{"Kafka", "Aerospike"}},
				map[string]any{"name": "project-b", "status": "completed", "technologies": []any{"Python", "Docker"}},
			},
		},
	})

	// update is an alias of write (parseOpKind). This second message is another
	// write, not a different operation.
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"user-1001","name":"SomeName M","simple_list":["red","green","blue","yellow"]}`),
	}))

	rec = outputRead(t, client, "user-1001")
	require.NotNil(t, rec)
	assert.Equal(t, "SomeName M", rec.Bins["name"])
	assert.Equal(t, []any{"red", "green", "blue", "yellow"}, rec.Bins["simple_list"])
	assert.Equal(t, "Bangalore", nestedString(t, rec.Bins["addresses"], 0, "city"))
	assert.Equal(t, "Aerospike", nestedString(t, rec.Bins["map_of_maps"], "work", "company"))
}

// TestIntegrationNestedBinVariations checks three JSON shapes on their own.
// The field name is the bin name. A later write to the same key replaces only
// the bins present in that message.
func TestIntegrationNestedBinVariations(t *testing.T) {
	w, client := outputSetup(t, "operation: write\n")

	tests := []struct {
		name   string
		id     string
		write  string
		update string
		want   map[string]any
	}{
		{
			name:   "string list",
			id:     "colors",
			write:  `{"id":"colors","label":"paint","simple_list":["red","green","blue"]}`,
			update: `{"id":"colors","simple_list":["red","green","blue","yellow"]}`,
			want: map[string]any{
				"label":       "paint",
				"simple_list": []any{"red", "green", "blue", "yellow"},
			},
		},
		{
			name:   "list of maps",
			id:     "places",
			write:  `{"id":"places","city":"Bangalore","addresses":[{"type":"home","zip":560001},{"type":"office","zip":600001}]}`,
			update: `{"id":"places","addresses":[{"type":"office","zip":600001}]}`,
			want: map[string]any{
				"city":      "Bangalore",
				"addresses": []any{map[string]any{"type": "office", "zip": 600001}},
			},
		},
		{
			name:   "map of lists",
			id:     "groups",
			write:  `{"id":"groups","label":"sets","groups":{"colors":["red","green"],"nums":[1,2,3]}}`,
			update: `{"id":"groups","label":"updated"}`,
			want: map[string]any{
				"label": "updated",
				"groups": map[string]any{
					"colors": []any{"red", "green"},
					"nums":   []any{1, 2, 3},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, tc.write)}))
			require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, tc.update)}))

			rec := outputRead(t, client, tc.id)
			require.NotNil(t, rec)
			assert.NotContains(t, rec.Bins, "id")
			assertNestedBins(t, rec.Bins, tc.want)
		})
	}
}

func assertNestedBins(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()
	assert.Equal(t, normalizeAerospike(want), normalizeAerospike(got))
}

func nestedString(t *testing.T, v any, path ...any) string {
	t.Helper()
	cur := normalizeAerospike(v)
	for _, p := range path {
		switch key := p.(type) {
		case int:
			cur = cur.([]any)[key]
		case string:
			cur = cur.(map[string]any)[key]
		}
	}
	s, ok := cur.(string)
	require.True(t, ok, "expected string at %v, got %T", path, cur)
	return s
}

// normalizeAerospike makes server CDTs comparable to the JSON we wrote.
// The client returns map keys as interface{} and integers as int or int64.
// A whole-number float stays a float: turning it into int would hide a bin
// stored as a double. A non-string map key keeps its type in the key text so
// it cannot compare equal to a string key.
func normalizeAerospike(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeAerospike(val)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[aerospikeMapKey(k)] = normalizeAerospike(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeAerospike(val)
		}
		return out
	case int:
		return t
	case int64:
		if t >= math.MinInt && t <= math.MaxInt {
			return int(t)
		}
		return t
	default:
		return v
	}
}

func aerospikeMapKey(k any) string {
	s, ok := k.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%T(%v)", k, k)
}

func TestNormalizeAerospikePreservesNumberAndKeyTypes(t *testing.T) {
	assert.Equal(t, float64(560001), normalizeAerospike(float64(560001)))
	assert.Equal(t, 10, normalizeAerospike(int64(10)))

	got := normalizeAerospike(map[any]any{1: "zip"}).(map[string]any)
	assert.NotContains(t, got, "1")
	assert.Equal(t, "zip", got["int(1)"])
}

// TestIntegrationDeleteKeyOnly deletes from a JSON body that carries only the
// key. A second delete of that missing key must succeed so a redelivery is not
// nacked forever.
func TestIntegrationDeleteKeyOnly(t *testing.T) {
	w, client := outputSetup(t, "operation: '${! meta(\"op\") }'\n")
	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "write", `{"id":"dk1","email":"a@b.com"}`)}))
	require.NotNil(t, outputRead(t, client, "dk1"))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "delete", `{"id":"dk1"}`)}))
	assert.Nil(t, outputRead(t, client, "dk1"))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "delete", `{"id":"dk1"}`)}))
}

func TestIntegrationCreateOnlyFailsWhenExists(t *testing.T) {
	w, client := outputSetup(t, "operation: create_only\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"co1","a":1}`)}))

	again := service.MessageBatch{msg(t, `{"id":"co1","a":2}`)}
	indexer := again.Index()
	err := w.WriteBatch(t.Context(), again)
	require.Error(t, err)
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "create_only")

	rec := outputRead(t, client, "co1")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"], "the failed create_only must not overwrite the record")
}

func TestIntegrationUpdateOnlyKeepsUntouchedBins(t *testing.T) {
	w, client := outputSetup(t, "operation: '${! meta(\"op\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "write", `{"id":"uo2","a":1,"b":2}`)}))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "update_only", `{"id":"uo2","a":9}`)}))

	rec := outputRead(t, client, "uo2")
	require.NotNil(t, rec)
	assert.Equal(t, 9, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"], "update_only must leave bins the message did not name")
}

func TestIntegrationUpdateOnlyFailsWhenMissing(t *testing.T) {
	w, client := outputSetup(t, "operation: update_only\n")

	batch := service.MessageBatch{msg(t, `{"id":"uo1","a":1}`)}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "update_only")
	assert.Nil(t, outputRead(t, client, "uo1"))
}

func TestIntegrationReplaceOnlyClearsOldBins(t *testing.T) {
	w, client := outputSetup(t, "operation: '${! meta(\"op\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "write", `{"id":"ro1","a":1,"b":2}`)}))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{opMsg(t, "replace_only", `{"id":"ro1","a":9}`)}))

	rec := outputRead(t, client, "ro1")
	require.NotNil(t, rec)
	assert.Equal(t, 9, rec.Bins["a"])
	assert.NotContains(t, rec.Bins, "b", "replace_only must drop bins not named in the write")
}

func TestIntegrationFencedDeleteThenReplaceOnly(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
operation: '${! meta("op").or("write") }'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	create := msg(t, `{"v":"live","extra":1}`)
	create.MetaSet("k", "fdro1")
	create.MetaSet("op", "write")
	create.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "fdro1")
	tombstone.MetaSet("off", "11")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))

	rec := outputRead(t, client, "fdro1")
	require.NotNil(t, rec, "a fenced delete must leave a tombstone record")
	assert.Equal(t, true, rec.Bins["_deleted"])

	repl := msg(t, `{"v":"back"}`)
	repl.MetaSet("k", "fdro1")
	repl.MetaSet("op", "replace_only")
	repl.MetaSet("off", "12")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{repl}))

	rec = outputRead(t, client, "fdro1")
	require.NotNil(t, rec)
	assert.Equal(t, "back", rec.Bins["v"])
	assert.Equal(t, 12, rec.Bins["_off"])
	assert.NotContains(t, rec.Bins, "_deleted", "replace_only must remove the tombstone")
	assert.NotContains(t, rec.Bins, "extra", "replace_only must drop bins it does not name")
}

func TestIntegrationFencedDeleteThenReplaceOnlyInOneBatch(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
operation: '${! meta("op").or("write") }'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	create := msg(t, `{"v":"live","extra":1}`)
	create.MetaSet("k", "fdro2")
	create.MetaSet("op", "write")
	create.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "fdro2")
	tombstone.MetaSet("off", "11")
	repl := msg(t, `{"v":"back"}`)
	repl.MetaSet("k", "fdro2")
	repl.MetaSet("op", "replace_only")
	repl.MetaSet("off", "12")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone, repl}))

	rec := outputRead(t, client, "fdro2")
	require.NotNil(t, rec)
	assert.Equal(t, "back", rec.Bins["v"], "the replace_only payload must survive coalescing with the delete")
	assert.Equal(t, 12, rec.Bins["_off"])
	assert.NotContains(t, rec.Bins, "_deleted")
	assert.NotContains(t, rec.Bins, "extra")
}

func TestIntegrationFencedDeleteThenUpdateOnlyInOneBatch(t *testing.T) {
	// update_only after a fenced delete folds to replace. The update carries a
	// nil tombstone bin, which REPLACE rejects, so the write omits it and the
	// replace drops the tombstone by not naming it.
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
operation: '${! meta("op").or("write") }'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	create := msg(t, `{"v":"live","extra":1}`)
	create.MetaSet("k", "fduo1")
	create.MetaSet("op", "write")
	create.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "fduo1")
	tombstone.MetaSet("off", "11")
	upd := msg(t, `{"v":"back"}`)
	upd.MetaSet("k", "fduo1")
	upd.MetaSet("op", "update_only")
	upd.MetaSet("off", "12")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone, upd}))

	rec := outputRead(t, client, "fduo1")
	require.NotNil(t, rec)
	assert.Equal(t, "back", rec.Bins["v"], "the update_only payload must survive coalescing with the delete")
	assert.Equal(t, 12, rec.Bins["_off"])
	assert.NotContains(t, rec.Bins, "_deleted")
	assert.NotContains(t, rec.Bins, "extra", "bins the delete removed must not survive the folded replace")
}

func TestIntegrationReplaceOnlyFailsWhenMissing(t *testing.T) {
	w, client := outputSetup(t, "operation: replace_only\n")

	batch := service.MessageBatch{msg(t, `{"id":"ro-missing","a":1}`)}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "replace_only")
	assert.Nil(t, outputRead(t, client, "ro-missing"))
}

func TestIntegrationNullBinsAndFoldedDelete(t *testing.T) {
	w, client := outputSetup(t, "operation: '${! meta(\"op\") }'\n")

	t.Run("null replace leaves the earlier write", func(t *testing.T) {
		batch := service.MessageBatch{
			opMsg(t, "write", `{"id":"wnr1","a":1}`),
			opMsg(t, "replace", `{"id":"wnr1","a":null}`),
		}
		indexer := batch.Index()
		err := w.WriteBatch(t.Context(), batch)
		require.Error(t, err)

		var batchErr *service.BatchError
		require.ErrorAs(t, err, &batchErr)
		assert.Equal(t, 1, batchErr.IndexedErrors())
		assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "only null bin values")

		rec := outputRead(t, client, "wnr1")
		require.NotNil(t, rec)
		assert.Equal(t, 1, rec.Bins["a"])
	})

	t.Run("replace then null write deletes", func(t *testing.T) {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"rnw1","a":1,"extra":2}`),
		}))
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "replace", `{"id":"rnw1","a":9}`),
			opMsg(t, "write", `{"id":"rnw1","a":null}`),
		}))
		assert.Nil(t, outputRead(t, client, "rnw1"))
	})

	t.Run("replace_only then null update deletes an existing record", func(t *testing.T) {
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"rou1","a":1,"extra":2}`),
		}))
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "replace_only", `{"id":"rou1","a":9}`),
			opMsg(t, "update_only", `{"id":"rou1","a":null}`),
		}))
		assert.Nil(t, outputRead(t, client, "rou1"))
	})

	t.Run("replace_only then null update fails when missing", func(t *testing.T) {
		batch := service.MessageBatch{
			opMsg(t, "replace_only", `{"id":"rou-missing","a":1}`),
			opMsg(t, "update_only", `{"id":"rou-missing","a":null}`),
		}
		indexer := batch.Index()
		err := w.WriteBatch(t.Context(), batch)
		require.Error(t, err)

		var batchErr *service.BatchError
		require.ErrorAs(t, err, &batchErr)
		assert.Equal(t, 2, batchErr.IndexedErrors())
		assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "replace_only")
		assert.Nil(t, outputRead(t, client, "rou-missing"))
	})

	t.Run("replace_only keeps a remaining bin", func(t *testing.T) {
		missing := service.MessageBatch{
			opMsg(t, "replace_only", `{"id":"rou-bin","a":1,"b":2}`),
			opMsg(t, "update_only", `{"id":"rou-bin","a":null}`),
		}
		indexer := missing.Index()
		err := w.WriteBatch(t.Context(), missing)
		require.Error(t, err)
		assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "replace_only")
		assert.Nil(t, outputRead(t, client, "rou-bin"))

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "write", `{"id":"rou-bin","a":1,"extra":7,"b":3}`),
		}))
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			opMsg(t, "replace_only", `{"id":"rou-bin","a":1,"b":2}`),
			opMsg(t, "update_only", `{"id":"rou-bin","a":null}`),
		}))
		rec := outputRead(t, client, "rou-bin")
		require.NotNil(t, rec)
		assert.Equal(t, 2, rec.Bins["b"])
		assert.NotContains(t, rec.Bins, "a")
		assert.NotContains(t, rec.Bins, "extra")
	})
}

func TestIntegrationPartialFailure(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"p1","ok":1}`),
		msg(t, `{"id":"p2","this_bin_name_is_much_too_long":1}`),
		msg(t, `{"id":"p3","ok":1}`),
	}

	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors(), "only the bad message should be failed")
	assert.Contains(t, err.Error(), "1 of 3")
	// The name is rejected while planning the batch, before any record is sent.
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "exceeds the Aerospike limit")

	assert.NotNil(t, outputRead(t, client, "p1"))
	assert.Nil(t, outputRead(t, client, "p2"))
	assert.NotNil(t, outputRead(t, client, "p3"))
}

// Listing 13 acknowledges a record the server rejects as too big. The oversized
// record is not stored, and the other record in the batch is.
func TestIntegrationIgnoreRecordTooBig(t *testing.T) {
	w, client := outputSetup(t, "ignore_error_codes: [13]\n")

	// Aerospike 8 rejects a record above the namespace max-record-size, which
	// defaults to 1MiB. The bin overhead puts this payload over that limit.
	oversized := strings.Repeat("x", 1100*1024)
	err := w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"too-big","blob":"`+oversized+`"}`),
		msg(t, `{"id":"kept","v":1}`),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), w.ignoredTotal.Load())
	assert.Nil(t, outputRead(t, client, "too-big"))
	assert.NotNil(t, outputRead(t, client, "kept"))
}

// Listing 21 acknowledges the local 15-byte rejection. The long-name record is
// still not stored, and the other records in the batch are.
func TestIntegrationIgnoreLongBinName(t *testing.T) {
	w, client := outputSetup(t, "ignore_error_codes: [21]\n")

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"p1","ok":1}`),
		msg(t, `{"id":"p2","this_bin_name_is_much_too_long":1}`),
		msg(t, `{"id":"p3","ok":1}`),
	})
	require.NoError(t, err)

	assert.NotNil(t, outputRead(t, client, "p1"))
	assert.Nil(t, outputRead(t, client, "p2"))
	assert.NotNil(t, outputRead(t, client, "p3"))
}

// Listing 20 acknowledges a namespace the cluster does not have. The client
// marks that record and still writes the rest of the batch.
func TestIntegrationIgnoreInvalidNamespace(t *testing.T) {
	w, client := outputSetup(t, `
namespace: '${! json("namespace_name") }'
ignore_error_codes: [20]
`)

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"ns20-bad","namespace_name":"not_a_namespace","v":1}`),
		msg(t, `{"id":"ns20-ok","namespace_name":"`+integrationNamespace+`","v":1}`),
	})
	require.NoError(t, err)
	assert.NotNil(t, outputRead(t, client, "ns20-ok"))
	assert.Nil(t, outputRead(t, client, "ns20-bad"))
}

// An unlisted 20 fails only that message. The record aimed at a real namespace
// in the same batch is stored.
func TestIntegrationInvalidNamespaceStillFailsWhenNotListed(t *testing.T) {
	w, client := outputSetup(t, `
namespace: '${! json("namespace_name") }'
`)

	batch := service.MessageBatch{
		msg(t, `{"id":"ns20b-bad","namespace_name":"not_a_namespace","v":1}`),
		msg(t, `{"id":"ns20b-ok","namespace_name":"`+integrationNamespace+`","v":1}`),
	}
	indexer := batch.Index()
	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "does not exist")
	assert.NotNil(t, outputRead(t, client, "ns20b-ok"))
	assert.Nil(t, outputRead(t, client, "ns20b-bad"))
}

// firstIndexedError returns the per-message error a batch failure carried, so a
// test can assert on why a message was nacked rather than only that it was.
func firstIndexedError(t *testing.T, indexer *service.Indexer, err error) error {
	t.Helper()

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)

	var found error
	batchErr.WalkMessagesIndexedBy(indexer, func(_ int, _ *service.Message, e error) bool {
		if e != nil && found == nil {
			found = e
		}
		return true
	})
	require.Error(t, found, "batch error carried no per-message error")
	return found
}

func TestIntegrationGenerationCheck(t *testing.T) {
	w, client := outputSetup(t, "generation: '${! meta(\"gen\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"g1","v":"first"}`)}))
	rec := outputRead(t, client, "g1")
	require.NotNil(t, rec)
	require.EqualValues(t, 1, rec.Generation)

	// The generation the caller read is still current, so the write lands.
	current := msg(t, `{"id":"g1","v":"second"}`)
	current.MetaSet("gen", "1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{current}))
	assert.Equal(t, "second", outputRead(t, client, "g1").Bins["v"])

	// The record has moved on since, so that same generation is now stale and
	// the write must be rejected rather than clobbering the newer value.
	stale := msg(t, `{"id":"g1","v":"third"}`)
	stale.MetaSet("gen", "1")
	staleBatch := service.MessageBatch{stale}
	indexer := staleBatch.Index()

	err := w.WriteBatch(t.Context(), staleBatch)
	require.Error(t, err)
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "generation check failed")
	assert.Equal(t, "second", outputRead(t, client, "g1").Bins["v"])
}

// A compare-and-set write must keep its check even when another message for the
// same key rides in the same batch. Coalescing the two would fold away the
// expected generation and acknowledge a write that silently clobbered whatever
// changed the record in between.
func TestIntegrationGenerationSurvivesCoalescing(t *testing.T) {
	w, client := outputSetup(t, "generation: '${! meta(\"gen\") }'\n")

	// Two unconditional writes, so generation 1 is definitely stale.
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"g2","v":"first"}`)}))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"g2","v":"second"}`)}))
	require.EqualValues(t, 2, outputRead(t, client, "g2").Generation)

	stale := msg(t, `{"id":"g2","v":"stale"}`)
	stale.MetaSet("gen", "1")
	// No gen metadata, so this one is an ordinary unconditional write.
	unchecked := msg(t, `{"id":"g2","other":true}`)

	batch := service.MessageBatch{stale, unchecked}
	indexer := batch.Index()

	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err, "a stale generation must not be dropped by coalescing")
	assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "generation check failed")

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors(), "only the compare-and-set message should fail")

	rec := outputRead(t, client, "g2")
	require.NotNil(t, rec)
	assert.Equal(t, "second", rec.Bins["v"], "the stale write must not have landed")
	assert.Equal(t, true, rec.Bins["other"], "the unchecked write must still have landed")
}

// Keys with no compare-and-set message still coalesce, so the exemption has not
// quietly disabled batching for everyone.
func TestIntegrationCoalescingStillFoldsWithoutGeneration(t *testing.T) {
	w, client := outputSetup(t, "generation: '${! meta(\"gen\") }'\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"g3","a":1}`),
		msg(t, `{"id":"g3","b":2}`),
	}))

	rec := outputRead(t, client, "g3")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"])
	// One folded command, so the record was created once rather than updated.
	assert.EqualValues(t, 1, rec.Generation)
}

// The warmed connection pool and the error-rate circuit breaker have to work
// against a real server, not merely parse.
func TestIntegrationClientPoolTuning(t *testing.T) {
	w, client := outputSetup(t, `
max_connections_per_node: 20
min_connections_per_node: 5
warm_up: true
max_error_rate: 10
error_rate_window: 2
`)

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"t1","v":"warm"}`)}))
	assert.Equal(t, "warm", outputRead(t, client, "t1").Bins["v"])
}

// An unlimited total timeout must leave the socket timeout in place rather than
// clamping it to zero and removing every bound on the command.
func TestIntegrationUnlimitedTotalTimeout(t *testing.T) {
	w, client := outputSetup(t, "total_timeout: 0s\nsocket_timeout: 5s\n")

	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"t2","v":"ok"}`)}))
	assert.Equal(t, "ok", outputRead(t, client, "t2").Bins["v"])
}
