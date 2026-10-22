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
	"encoding/base64"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// newTestWriter builds a writer from YAML, which exercises the config spec and
// the parsing together rather than hand-assembling a config struct that could
// drift from what a real pipeline produces.
func newTestWriter(t *testing.T, yaml string) *aerospikeWriter {
	t.Helper()
	conf, err := outputSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	parsed, err := parseOutputConfig(conf)
	require.NoError(t, err)
	// Mirror newAerospikeOutput, so tests see the pool the pipeline would get.
	maxInFlight, err := conf.FieldMaxInFlight()
	require.NoError(t, err)
	parsed.client.sizePoolForConcurrency(maxInFlight)
	return &aerospikeWriter{conf: parsed, conn: newConnection(parsed.client, nil)}
}

// mapOne maps a single message through the batch mapper, which is how the
// writer resolves every interpolated field.
func mapOne(t *testing.T, w *aerospikeWriter, m *service.Message) (*pendingOp, error) {
	t.Helper()
	return newBatchMapper(w.conf, service.MessageBatch{m}).mapMessage(0)
}

const baseConfig = `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("id") }'
bins: 'root = this.without("id")'
`

func TestParseTTL(t *testing.T) {
	tests := []struct {
		in      string
		want    uint32
		wantErr bool
	}{
		{"0s", as.TTLServerDefault, false},
		{"0", as.TTLServerDefault, false},
		{"", as.TTLServerDefault, false},
		{"never", as.TTLDontExpire, false},
		{"-1", as.TTLDontExpire, false},
		{"keep", as.TTLDontUpdate, false},
		{"-2", as.TTLDontUpdate, false},
		{"24h", 86400, false},
		{"90s", 90, false},
		// Case-insensitive S/M/H/D units, days, and bare seconds.
		{"24H", 86400, false},
		{" 24H ", 86400, false},
		{"1D", 86400, false},
		{"1d", 86400, false},
		{"12D", 12 * 86400, false},
		{"5M", 300, false},
		{"5m", 300, false},
		{"60S", 60, false},
		{"3600", 3600, false},
		{"1H", 3600, false},
		// A zero duration is the namespace default, including a unit and -0s.
		{"0m", as.TTLServerDefault, false},
		{"0H", as.TTLServerDefault, false},
		{"0ms", as.TTLServerDefault, false},
		{"-0s", as.TTLServerDefault, false},
		// Rounding a positive sub-second TTL to zero would silently mean
		// "namespace default", which is the opposite of what was asked for.
		{"500ms", 0, true},
		{"-5s", 0, true},
		{"-3", 0, true},
		{"nonsense", 0, true},
		{"1 d", 0, true},
		{"24X", 0, true},
		{"-1m", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseTTL(tc.in)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseTTLNegativeUnitsShareOneError(t *testing.T) {
	dayErr := requireParseTTLError(t, "-1d")
	minuteErr := requireParseTTLError(t, "-1m")
	assert.Equal(t, `invalid ttl "-1d": must not be negative`, dayErr)
	assert.Equal(t, `invalid ttl "-1m": must not be negative`, minuteErr)
}

func requireParseTTLError(t *testing.T, in string) string {
	t.Helper()
	_, err := parseTTL(in)
	require.Error(t, err)
	return err.Error()
}

func TestMapMessageTTLUnits(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\nttl: '${! json(\"ttl\") }'\n")

	msg := service.NewMessage([]byte(`{"id":"u1","name":"Ada","ttl":"24H"}`))
	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(86400), op.ttl)

	msg = service.NewMessage([]byte(`{"id":"u1","name":"Ada","ttl":3600}`))
	op, err = mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(3600), op.ttl)
}

func TestMapMessageTTLUnitsRejectsInvalid(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\nttl: '${! json(\"ttl\") }'\n")

	msg := service.NewMessage([]byte(`{"id":"u1","name":"Ada","ttl":"24X"}`))
	_, err := mapOne(t, w, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ttl")
}

func TestParseOpKind(t *testing.T) {
	for in, want := range map[string]opKind{
		"write":        opWrite,
		"update":       opWrite,
		"replace":      opReplace,
		"replace_only": opReplaceOnly,
		"create_only":  opCreateOnly,
		"update_only":  opUpdateOnly,
		"delete":       opDelete,
		"DELETE":       opDelete,
	} {
		got, err := parseOpKind(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	_, err := parseOpKind("upsert")
	assert.Error(t, err)
}

func TestMapMessage(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage([]byte(`{"id":"u1","name":"Ada","score":10}`))
	op, err := mapOne(t, w, msg)
	require.NoError(t, err)

	assert.Equal(t, opWrite, op.kind)
	assert.Equal(t, "test", op.key.Namespace())
	assert.Equal(t, "users", op.key.SetName())
	assert.Equal(t, "u1", op.key.Value().GetObject())
	assert.Equal(t, []string{"name", "score"}, op.binOrder)
	assert.Equal(t, "Ada", op.bins["name"])
	assert.Equal(t, int64(10), op.bins["score"])
}

func TestMapMessageRejectsLongBinName(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage([]byte(`{"id":"u1","this_field_name_is_far_too_long":1}`))
	_, err := mapOne(t, w, msg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")
}

func TestMapMessageTombstoneUsesKafkaKey(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("user_id").catch(meta("kafka_key")) }'
bins: 'root = this.without("user_id", "ttl")'
operation: replace
ttl: '${! json("ttl").or("keep") }'
tombstone_as_delete: true
`)

	tombstone := service.NewMessage([]byte(``))
	tombstone.MetaSet("kafka_key", "u-42")
	op, err := mapOne(t, w, tombstone)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assert.Equal(t, "u-42", op.key.Value().GetObject())

	body := msg(t, `{"user_id":"u-42","email":"a@b.com","ttl":"24H"}`)
	body.MetaSet("kafka_key", "other")
	op, err = mapOne(t, w, body)
	require.NoError(t, err)
	assert.Equal(t, opReplace, op.kind)
	assert.Equal(t, "u-42", op.key.Value().GetObject())
}

func TestMapMessageTombstoneBecomesDelete(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! meta("k") }'
bins: 'root = this'
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assert.Empty(t, op.binOrder)
}

func TestMapMessageTombstoneDisabledStaysWrite(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! meta("k") }'
bins: 'root.kept = "yes"'
tombstone_as_delete: false
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opWrite, op.kind)
	assert.Equal(t, "u1", op.key.Value().GetObject())
	assert.Equal(t, []string{"kept"}, op.binOrder)
	assert.Equal(t, "yes", op.bins["kept"])
}

func TestMapMessageStructuredObjectIsNotTombstone(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{"id": "u1", "v": 1})

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opWrite, op.kind)
}

func TestMapMessageDynamicOperation(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("id") }'
operation: '${! if json("op") == "d" { "delete" } else { "write" } }'
tombstone_as_delete: false
bins: 'root = this.without("id", "op")'
`)

	del, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","op":"d"}`)))
	require.NoError(t, err)
	assert.Equal(t, opDelete, del.kind)

	upd, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","op":"c","v":1}`)))
	require.NoError(t, err)
	assert.Equal(t, opWrite, upd.kind)
}

func TestMapMessageJSONUserRecord(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("user_id") }'
bins: 'root = this.without("user_id")'
operation: replace
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"user_id":"u-42","email":"a@b.com","plan":"pro"}`)))
	require.NoError(t, err)
	assert.Equal(t, "u-42", op.key.Value().GetObject())
	assert.Equal(t, "test", op.key.Namespace())
	assert.Equal(t, "users", op.key.SetName())
	assert.Equal(t, opReplace, op.kind)
	assert.Equal(t, map[string]any{"email": "a@b.com", "plan": "pro"}, op.bins)
	assert.NotContains(t, op.bins, "user_id")
}

func TestMapMessageTopicIsNamespace(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: '${! meta("kafka_topic") }'
set: users
key: '${! meta("kafka_key") }'
bins: 'root = this'
`)

	msg := service.NewMessage([]byte(`{"email":"a@b.com"}`))
	msg.MetaSet("kafka_topic", "payments")
	msg.MetaSet("kafka_key", "u-42")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, "payments", op.key.Namespace())
	assert.Equal(t, "users", op.key.SetName())
	assert.Equal(t, "u-42", op.key.Value().GetObject())
	assert.Equal(t, map[string]any{"email": "a@b.com"}, op.bins)
}

func TestMapMessageNamespaceAndSetFromJSONFields(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: '${! json("namespace_name") }'
set: '${! json("set_name") }'
key: '${! json("id") }'
bins: 'root = this.without("id", "namespace_name", "set_name")'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"e-1","namespace_name":"payments","set_name":"clicks","n":1}`)))
	require.NoError(t, err)
	assert.Equal(t, "payments", op.key.Namespace())
	assert.Equal(t, "clicks", op.key.SetName())
	assert.Equal(t, "e-1", op.key.Value().GetObject())
	assert.Equal(t, map[string]any{"n": int64(1)}, op.bins)
	assert.NotContains(t, op.bins, "namespace_name")
	assert.NotContains(t, op.bins, "set_name")
}

func TestMapMessageRejectsMissingNamespaceField(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: '${! json("namespace_name") }'
set: '${! json("set_name") }'
key: '${! json("id") }'
bins: 'root = this.without("id", "namespace_name", "set_name")'
`)

	_, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"e-1"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `field 'namespace'`)
	assert.Contains(t, err.Error(), "null")
}

func TestMapMessageRejectsSetOutsideAllowList(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: '${! if ["clicks", "views"].contains(json("set_name")) { json("set_name") } else { throw("set_name is not allowed") } }'
key: '${! json("id") }'
bins: 'root = this.without("id", "set_name")'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"e-1","set_name":"clicks","n":1}`)))
	require.NoError(t, err)
	assert.Equal(t, "clicks", op.key.SetName())

	_, err = mapOne(t, w, service.NewMessage([]byte(`{"id":"e-1","set_name":"nope","n":1}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set_name is not allowed")
}

func TestMapMessageIntKeyType(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
key_type: int
bins: 'root = this.without("id")'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"7","v":1}`)))
	require.NoError(t, err)
	assert.Equal(t, int64(7), op.key.Value().GetObject())

	_, err = mapOne(t, w, service.NewMessage([]byte(`{"id":"abc","v":1}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")
}

func TestMapMessageFencing(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
bins: 'root = this.without("id")'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(`{"id":"u1","v":1}`))
	msg.MetaSet("kafka_offset", "42")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.True(t, op.hasFence)
	assert.Equal(t, int64(42), op.fence)
	assert.Equal(t, int64(42), op.bins["_off"])
}

func TestMapMessageFencedDeleteWritesTombstone(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! meta("k") }'
bins: 'root = this'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")
	msg.MetaSet("kafka_offset", "9")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assert.True(t, op.hasFence)
	assert.Equal(t, int64(9), op.fence)
	assert.Equal(t, int64(9), op.bins["_off"])
	assert.Equal(t, true, op.bins["_deleted"])
}

func TestConfigRejectsFenceBinMatchingTombstoneBin(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
fencing:
  enabled: true
  bin: _deleted
  tombstone_bin: _deleted
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be the same")
}

func TestMapMessageRejectsEmptyKey(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	_, err := mapOne(t, w, service.NewMessage([]byte(`{"name":"Ada"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a usable record key")
}

func TestMapMessageRejectsEmptyBins(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
tombstone_as_delete: false
bins: 'root = {}'
`)

	_, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "produced no bins")
}

func TestConfigRejectsBadStaticOperation(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
operation: upsert
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation")
}

func TestMapMessageGeneration(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
bins: 'root = this.without("id")'
generation: '${! meta("aerospike_generation") }'
`)

	msg := service.NewMessage([]byte(`{"id":"u1","v":1}`))
	msg.MetaSet("aerospike_generation", "7")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.True(t, op.hasGeneration)
	assert.Equal(t, uint32(7), op.generation)

	rec := w.buildRecord(op)
	write, ok := rec.(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.EXPECT_GEN_EQUAL, write.Policy.GenerationPolicy)
	assert.Equal(t, uint32(7), write.Policy.Generation)
}

func TestMapMessageOmitsGenerationWhenEmpty(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","v":1}`)))
	require.NoError(t, err)
	assert.False(t, op.hasGeneration)

	rec := w.buildRecord(op)
	write, ok := rec.(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.NONE, write.Policy.GenerationPolicy)
}

func TestConfigRejectsLongFenceBin(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
fencing:
  enabled: true
  bin: this_bin_name_is_too_long
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")
}

func TestParseConfigSkillDefaults(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	assert.Equal(t, uint32(as.TTLDontUpdate), w.conf.staticTTL)
	assert.Equal(t, 0, w.conf.batchPolicy.MaxRetries)
	assert.Equal(t, as.COMMIT_ALL, w.conf.writePolicy.CommitLevel)
	assert.Equal(t, as.COMMIT_ALL, w.conf.deletePolicy.CommitLevel)
	assert.Equal(t, 0, w.conf.maxRecordBytes)
	assert.Empty(t, w.conf.ignoreCodes)
}

func TestParseIgnoreErrorCodes(t *testing.T) {
	w := newTestWriter(t, baseConfig+"ignore_error_codes: [13, 21]\n")
	assert.True(t, w.conf.ignores(13))
	assert.True(t, w.conf.ignores(21))
	assert.False(t, w.conf.ignores(types.KEY_BUSY))
}

func TestParseCommitLevelMaster(t *testing.T) {
	w := newTestWriter(t, baseConfig+"commit_level: master\n")
	assert.Equal(t, as.COMMIT_MASTER, w.conf.writePolicy.CommitLevel)
	assert.Equal(t, as.COMMIT_MASTER, w.conf.deletePolicy.CommitLevel)
}

func TestMapMessageRejectsOversizeRecord(t *testing.T) {
	w := newTestWriter(t, baseConfig+"max_record_bytes: 32\n")
	_, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","blob":"abcdefghijklmnopqrstuvwxyz0123456789"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_record_bytes")
}

func assertDigestKey(t *testing.T, key *as.Key, want []byte) {
	t.Helper()
	require.NotNil(t, key.Value())
	assert.Nil(t, key.Value().GetObject())
	assert.Equal(t, want, key.Digest())
}

func TestDigestBase64JSONField(t *testing.T) {
	raw := bytes.Repeat([]byte{0x11}, digestLen)
	b64 := base64.StdEncoding.EncodeToString(raw)
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '${! json("digest") }'
bins: 'root = this.without("digest")'
`)

	body := `{"digest":"` + b64 + `","name":"Ada"}`
	op, err := mapOne(t, w, service.NewMessage([]byte(body)))
	require.NoError(t, err)
	assertDigestKey(t, op.key, raw)
	assert.Equal(t, "Ada", op.bins["name"])
	assert.NotContains(t, op.bins, "digest")

	hashed, err := as.NewKey("test", "users", b64)
	require.NoError(t, err)
	assert.NotEqual(t, hashed.Digest(), op.key.Digest())

	other := service.NewMessage([]byte(`{"digest":"` + b64 + `","name":"Grace"}`))
	batch := service.MessageBatch{service.NewMessage([]byte(body)), other}
	resolver := w.conf.keys.resolver(batch)
	first, err := resolver.resolve(0)
	require.NoError(t, err)
	second, err := resolver.resolve(1)
	require.NoError(t, err)
	assert.Equal(t, keyID(first), keyID(second))
}

func TestDigestContentBase64(t *testing.T) {
	raw := bytes.Repeat([]byte{0x22}, digestLen)
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '${! content() }'
bins: 'root.name = "Ada"'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(base64.StdEncoding.EncodeToString(raw))))
	require.NoError(t, err)
	assertDigestKey(t, op.key, raw)
}

func TestDigestRawBytesEncodedInMapping(t *testing.T) {
	// Not valid UTF-8, so a lossy string conversion would change the digest.
	raw := bytes.Repeat([]byte{0xff, 0x00, 0x80, 0xc3}, digestLen/4)

	tests := []struct {
		name   string
		digest string
		msg    func() *service.Message
	}{
		{
			name:   "payload",
			digest: `${! content().encode("base64") }`,
			msg:    func() *service.Message { return service.NewMessage(append([]byte(nil), raw...)) },
		},
		{
			name:   "kafka key",
			digest: `${! meta("kafka_key").encode("base64") }`,
			msg: func() *service.Message {
				m := service.NewMessage(nil)
				m.MetaSetMut("kafka_key", append([]byte(nil), raw...))
				return m
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '`+tc.digest+`'
bins: 'root.name = "Ada"'
tombstone_as_delete: false
`)
			op, err := mapOne(t, w, tc.msg())
			require.NoError(t, err)
			assertDigestKey(t, op.key, raw)
		})
	}
}

func TestDigestRejectsTwentyByteText(t *testing.T) {
	// A 20-byte string is the length of a raw digest but is not Base64 of one.
	// It must be rejected rather than used as raw bytes, which would address an
	// arbitrary record.
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '${! meta("kafka_key") }'
bins: 'root.name = "Ada"'
`)

	for _, kafkaKey := range []string{"abcdefghijklmnopqrst", "user-000000000000001"} {
		msg := service.NewMessage(nil)
		msg.MetaSetMut("kafka_key", kafkaKey)
		_, err := mapOne(t, w, msg)
		require.Error(t, err, kafkaKey)
	}
}

func TestDigestUsesPipelineBloblangEnvironment(t *testing.T) {
	raw := bytes.Repeat([]byte{0x44}, digestLen)
	benv := bloblang.GlobalEnvironment().WithoutFunctions()
	require.NoError(t, benv.RegisterFunctionV2("test_digest", bloblang.NewPluginSpec(),
		func(*bloblang.ParsedParams) (bloblang.Function, error) {
			return func() (any, error) { return base64.StdEncoding.EncodeToString(raw), nil }, nil
		}))
	env := service.NewEnvironment()
	env.UseBloblangEnvironment(benv)

	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
digest: '${! test_digest() }'
bins: 'root.name = "Ada"'
`, env)
	require.NoError(t, err)
	parsed, err := parseOutputConfig(conf)
	require.NoError(t, err)

	key, err := parsed.keys.resolver(service.MessageBatch{service.NewMessage(nil)}).resolve(0)
	require.NoError(t, err)
	assertDigestKey(t, key, raw)
}

func TestDigestTombstoneUsesMetadata(t *testing.T) {
	raw := bytes.Repeat([]byte{0x7f}, digestLen)
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '${! meta("as_digest") }'
bins: 'root = this'
tombstone_as_delete: true
`)

	msg := service.NewMessage(nil)
	msg.MetaSetMut("as_digest", base64.StdEncoding.EncodeToString(raw))
	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assertDigestKey(t, op.key, raw)
}

func TestDigestRejectsWrongTypesAndLengths(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '${! json("digest") }'
bins: 'root = this.without("digest")'
`)

	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "number", body: `{"digest":1}`, want: "not standard Base64"},
		{name: "missing", body: `{"name":"Ada"}`, want: `"null"`},
		{name: "not base64", body: `{"digest":"!!!!"}`, want: "not standard Base64"},
		{name: "wrong decoded length", body: `{"digest":"YWI="}`, want: "decoded to 2 bytes"},
		{name: "twenty characters", body: `{"digest":"abcdefghijklmnopqrst"}`, want: "decoded to 15 bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapOne(t, w, service.NewMessage([]byte(tc.body)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestDigestStaticValue(t *testing.T) {
	raw := bytes.Repeat([]byte{0x33}, digestLen)
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
digest: '`+base64.StdEncoding.EncodeToString(raw)+`'
bins: 'root.name = "Ada"'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{}`)))
	require.NoError(t, err)
	assertDigestKey(t, op.key, raw)
}

func TestDigestConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "both",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
key: id
digest: '${! json("digest") }'
`,
			want: "exactly one",
		},
		{
			name: "neither",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
`,
			want: "exactly one",
		},
		{
			name: "key type",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
digest: '${! json("digest") }'
key_type: bytes
`,
			want: "key_type",
		},
		{
			name: "key encoding",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
digest: '${! json("digest") }'
key_encoding: hex
`,
			want: "key_encoding",
		},
		{
			name: "static not base64",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
digest: '!!!!'
`,
			want: "not standard Base64",
		},
		{
			name: "static length",
			yaml: `
hosts: [ "localhost:3000" ]
namespace: test
digest: 'YWI='
`,
			want: "decoded to 2 bytes",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := outputSpec().ParseYAML(tc.yaml, nil)
			require.NoError(t, err)
			_, err = parseOutputConfig(conf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
