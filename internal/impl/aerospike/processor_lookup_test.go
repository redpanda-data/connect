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
	"crypto/tls"
	"log/slog"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func newTestProcessor(t *testing.T, yaml string) *lookupProcessor {
	t.Helper()
	conf, err := lookupSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	parsed, err := parseLookupConfig(conf)
	require.NoError(t, err)
	// Mirror newLookupProcessor, so tests see the pool the pipeline would get.
	parsed.client.sizePoolForConcurrency(defaultLookupConcurrency)
	return &lookupProcessor{conf: parsed, conn: newConnection(parsed.client, nil)}
}

const lookupBaseConfig = `
hosts: [ "localhost:3000" ]
namespace: profiles
set: user
key: '${! json("user_id") }'
`

// TestPlanReadsDeduplicates is the efficiency claim: a stream of events about a
// few entities must not issue one read per event.
func TestPlanReadsDeduplicates(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"user_id":"u1","evt":1}`)),
		service.NewMessage([]byte(`{"user_id":"u2","evt":2}`)),
		service.NewMessage([]byte(`{"user_id":"u1","evt":3}`)),
		service.NewMessage([]byte(`{"user_id":"u1","evt":4}`)),
	}

	reads := p.planReads(batch)

	// Four messages, two entities, two reads.
	require.Len(t, reads, 2)
	assert.Equal(t, []int{0, 2, 3}, reads[0].indexes)
	assert.Equal(t, []int{1}, reads[1].indexes)
}

func TestPlanReadsIsolatesKeyFailures(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"user_id":"u1"}`)),
		service.NewMessage([]byte(`{"no_key":true}`)),
		service.NewMessage([]byte(`{"user_id":"u3"}`)),
	}

	reads := p.planReads(batch)

	assert.Len(t, reads, 2)

	// The bad message carries its own error and keeps its content.
	require.Error(t, batch[1].GetError())
	assert.Contains(t, batch[1].GetError().Error(), "not a usable record key")
}

func TestReadAllBinsWhenNoneNamed(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)
	assert.True(t, p.conf.readAllBins)

	batch := service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1"}`))}
	reads := p.planReads(batch)
	require.Len(t, reads, 1)
	// Without this the client reads only the record header, returning no bins.
	assert.True(t, reads[0].read.ReadAllBins)
}

func TestNamedBinsAreRequested(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig+"bins: [ tier, ltv ]\n")
	assert.False(t, p.conf.readAllBins)

	batch := service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1"}`))}
	reads := p.planReads(batch)
	require.Len(t, reads, 1)
	assert.False(t, reads[0].read.ReadAllBins)
	assert.Equal(t, []string{"tier", "ltv", defaultTombstoneBin}, reads[0].read.BinNames)
}

func TestConfigRejectsLongBinName(t *testing.T) {
	conf, err := lookupSpec().ParseYAML(lookupBaseConfig+"bins: [ this_bin_name_is_too_long ]\n", nil)
	require.NoError(t, err)

	_, err = parseLookupConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")
}

func TestLookupRequiresUserKey(t *testing.T) {
	// digest is an output field. Lookup still requires a user key, and a digest
	// value does not satisfy that requirement.
	_, err := lookupSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
digest: '${! json("digest") }'
`, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field 'key' is required")
}

func TestLookupWarnsAtStartup(t *testing.T) {
	insecure := suiteNamed(t, tls.InsecureCipherSuites(), "TLS_RSA_WITH_AES_128_CBC_SHA")

	tests := []struct {
		name  string
		extra string
		want  []string
	}{
		{
			name:  "insecure cipher suite",
			extra: "tls:\n  enabled: true\nciphers: [ \"" + insecure.Name + "\" ]\n",
			want:  []string{"insecure cipher suites", insecure.Name},
		},
		{
			name:  "cert refresh without a certificate file",
			extra: "cert_refresh_interval: 1h\n",
			want:  []string{"cert_refresh_interval", "has no effect"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
			mgr := service.MockResources(service.MockResourcesOptUseLogger(logger))

			conf, err := lookupSpec().ParseYAML(lookupBaseConfig+tc.extra, nil)
			require.NoError(t, err)
			p, err := newLookupProcessor(conf, mgr)
			require.NoError(t, err)
			t.Cleanup(func() { _ = p.Close(context.Background()) })

			for _, want := range tc.want {
				assert.Contains(t, buf.String(), want)
			}
		})
	}
}

func TestAssembleRemovesDropped(t *testing.T) {
	batch := service.MessageBatch{
		service.NewMessage([]byte(`a`)),
		service.NewMessage([]byte(`b`)),
		service.NewMessage([]byte(`c`)),
	}

	out := assemble(batch, map[int]bool{1: true})
	require.Len(t, out, 1)
	require.Len(t, out[0], 2)

	first, _ := out[0][0].AsBytes()
	second, _ := out[0][1].AsBytes()
	assert.Equal(t, "a", string(first))
	assert.Equal(t, "c", string(second))
}

func TestNotFoundModes(t *testing.T) {
	t.Run("emit_null", func(t *testing.T) {
		p := newTestProcessor(t, lookupBaseConfig)
		m := service.NewMessage([]byte(`{"user_id":"u1"}`))

		assert.Equal(t, resultKeep, p.applyNotFound(m))
		v, err := m.AsStructured()
		require.NoError(t, err)
		assert.Nil(t, v)
		assert.NoError(t, m.GetError())
	})

	t.Run("error", func(t *testing.T) {
		p := newTestProcessor(t, lookupBaseConfig+"not_found: error\n")
		m := service.NewMessage([]byte(`{"user_id":"u1"}`))

		assert.Equal(t, resultKeep, p.applyNotFound(m))
		require.Error(t, m.GetError())
	})

	t.Run("drop", func(t *testing.T) {
		p := newTestProcessor(t, lookupBaseConfig+"not_found: drop\n")
		m := service.NewMessage([]byte(`{"user_id":"u1"}`))

		assert.Equal(t, resultDrop, p.applyNotFound(m))
	})
}

func TestTombstoneRecordIsNotFound(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)
	m := service.NewMessage([]byte(`{"user_id":"u1"}`))

	rec := &as.BatchRecord{
		ResultCode: types.OK,
		Record:     &as.Record{Bins: as.BinMap{defaultTombstoneBin: true, "tier": "gold"}},
	}
	assert.Equal(t, resultKeep, p.applyResult(m, rec))
	v, err := m.AsStructured()
	require.NoError(t, err)
	assert.Nil(t, v)
	assert.NoError(t, m.GetError())
}

func TestApplyResultSuccessEmitsMetadata(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig+"emit_metadata: true\n")
	m := service.NewMessage([]byte(`{"user_id":"u1"}`))

	rec := &as.BatchRecord{
		ResultCode: types.OK,
		Record: &as.Record{
			Bins:       as.BinMap{"tier": "gold"},
			Generation: 4,
			Expiration: 99,
		},
	}
	assert.Equal(t, resultKeep, p.applyResult(m, rec))
	v, err := m.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"tier": "gold"}, v)

	gen, ok := m.MetaGet(metaGeneration)
	require.True(t, ok)
	assert.Equal(t, "4", gen)

	// The metadata is documented as feeding the output's ttl field, so it has
	// to be in a form that field accepts.
	ttl, ok := m.MetaGet(metaTTL)
	require.True(t, ok)
	assert.Equal(t, "1m39s", ttl)
	parsed, err := parseTTL(ttl)
	require.NoError(t, err)
	assert.Equal(t, uint32(99), parsed)
}

// A record that never expires reports a sentinel rather than a duration, which
// would otherwise reach the user as the meaningless string "4294967295".
func TestApplyResultNeverExpiringTTLMetadata(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig+"emit_metadata: true\n")
	m := service.NewMessage([]byte(`{"user_id":"u1"}`))

	rec := &as.BatchRecord{
		ResultCode: types.OK,
		Record: &as.Record{
			Bins:       as.BinMap{"tier": "gold"},
			Expiration: as.TTLDontExpire,
		},
	}
	require.Equal(t, resultKeep, p.applyResult(m, rec))

	ttl, ok := m.MetaGet(metaTTL)
	require.True(t, ok)
	assert.Equal(t, "never", ttl)

	parsed, err := parseTTL(ttl)
	require.NoError(t, err)
	assert.Equal(t, uint32(as.TTLDontExpire), parsed)
}

// Fencing bookkeeping is not part of the record's data. Leaving it in place
// grafts an internal field onto every enriched message.
func TestApplyResultStripsFencingBins(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)
	m := service.NewMessage([]byte(`{"user_id":"u1"}`))

	rec := &as.BatchRecord{
		ResultCode: types.OK,
		Record: &as.Record{
			Bins: as.BinMap{"tier": "gold", defaultFenceBin: int64(42)},
		},
	}
	require.Equal(t, resultKeep, p.applyResult(m, rec))

	v, err := m.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"tier": "gold"}, v)
}

func TestApplyResultKeepsFenceBinWhenCheckDisabled(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig+"fence_bin: ''\n")
	m := service.NewMessage([]byte(`{"user_id":"u1"}`))

	rec := &as.BatchRecord{
		ResultCode: types.OK,
		Record: &as.Record{
			Bins: as.BinMap{"tier": "gold", defaultFenceBin: int64(42)},
		},
	}
	require.Equal(t, resultKeep, p.applyResult(m, rec))

	v, err := m.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"tier": "gold", defaultFenceBin: int64(42)}, v)
}

func TestParseReadPolicy(t *testing.T) {
	p := newTestProcessor(t, lookupBaseConfig)
	assert.Equal(t, 2, p.conf.batchPolicy.MaxRetries)
	assert.Equal(t, as.SEQUENCE, p.conf.batchPolicy.ReplicaPolicy)
	assert.Equal(t, as.ReadModeAPOne, p.conf.batchPolicy.ReadModeAP)
	assert.Equal(t, as.ReadModeSCSession, p.conf.batchPolicy.ReadModeSC)

	p = newTestProcessor(t, lookupBaseConfig+`
replica: master_proles
read_mode_ap: all
read_mode_sc: linearize
`)
	assert.Equal(t, as.MASTER_PROLES, p.conf.batchPolicy.ReplicaPolicy)
	assert.Equal(t, as.ReadModeAPAll, p.conf.batchPolicy.ReadModeAP)
	assert.Equal(t, as.ReadModeSCLinearize, p.conf.batchPolicy.ReadModeSC)
}
