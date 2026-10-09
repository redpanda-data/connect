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

package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type fakeKVEntry struct {
	jetstream.KeyValueEntry
	key      string
	bucket   string
	value    []byte
	revision uint64
	delta    uint64
	created  time.Time
	op       jetstream.KeyValueOp
}

func (f *fakeKVEntry) Key() string                     { return f.key }
func (f *fakeKVEntry) Bucket() string                  { return f.bucket }
func (f *fakeKVEntry) Value() []byte                   { return f.value }
func (f *fakeKVEntry) Revision() uint64                { return f.revision }
func (f *fakeKVEntry) Delta() uint64                   { return f.delta }
func (f *fakeKVEntry) Created() time.Time              { return f.created }
func (f *fakeKVEntry) Operation() jetstream.KeyValueOp { return f.op }

type fakeKV struct {
	jetstream.KeyValue
	entry       jetstream.KeyValueEntry
	gotKey      string
	gotRevision uint64
}

func (f *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	f.gotKey = key
	return f.entry, nil
}

func (f *fakeKV) GetRevision(_ context.Context, key string, revision uint64) (jetstream.KeyValueEntry, error) {
	f.gotKey = key
	f.gotRevision = revision
	return f.entry, nil
}

func TestKVProcessorGetPreservesInputMetadata(t *testing.T) {
	created := time.Now()

	for _, op := range []kvpOperationType{kvpOperationGet, kvpOperationGetRevision} {
		t.Run(string(op), func(t *testing.T) {
			key, err := service.NewInterpolatedString("testkey")
			require.NoError(t, err)

			kv := &fakeKV{entry: &fakeKVEntry{
				key:      "testkey",
				bucket:   "testbucket",
				value:    []byte("stored"),
				revision: 5,
				delta:    2,
				created:  created,
				op:       jetstream.KeyValuePut,
			}}
			p := &kvProcessor{
				operation: op,
				key:       key,
				timeout:   time.Second,
				kv:        kv,
			}
			if op == kvpOperationGetRevision {
				revision, err := service.NewInterpolatedString("5")
				require.NoError(t, err)
				p.revision = revision
			}

			msg := service.NewMessage([]byte("original"))
			msg.MetaSetMut("custom_meta", "custom_value")

			batch, err := p.Process(t.Context(), msg)
			require.NoError(t, err)
			require.Len(t, batch, 1)
			out := batch[0]

			got, ok := out.MetaGetMut("custom_meta")
			require.True(t, ok, "metadata set before the processor must survive")
			assert.Equal(t, "custom_value", got)

			b, err := out.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, "stored", string(b))

			wantMeta := map[string]any{
				metaKVKey:       "testkey",
				metaKVBucket:    "testbucket",
				metaKVRevision:  uint64(5),
				metaKVDelta:     uint64(2),
				metaKVOperation: jetstream.KeyValuePut.String(),
				metaKVCreated:   created,
			}
			for k, want := range wantMeta {
				got, ok := out.MetaGetMut(k)
				require.True(t, ok, k)
				assert.Equal(t, want, got, k)
			}

			assert.Equal(t, "testkey", kv.gotKey)
			if op == kvpOperationGetRevision {
				assert.Equal(t, uint64(5), kv.gotRevision)
			} else {
				assert.Zero(t, kv.gotRevision)
			}
		})
	}
}
