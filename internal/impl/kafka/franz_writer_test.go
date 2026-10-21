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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestFranzWriterTombstone(t *testing.T) {
	spec := service.NewConfigSpec().Fields(FranzWriterConfigFields()...)

	newMsg := func(body []byte, meta map[string]any) *service.Message {
		msg := service.NewMessage(body)
		for k, v := range meta {
			msg.MetaSetMut(k, v)
		}
		return msg
	}

	testCases := []struct {
		name        string
		conf        string
		msg         *service.Message
		wantValue   []byte
		wantNil     bool
		errContains string
	}{
		{
			name:      "field unset keeps empty payload as non-null",
			conf:      `topic: foo`,
			msg:       newMsg([]byte{}, nil),
			wantValue: []byte{},
		},
		{
			name: "mirrored tombstone stays a tombstone",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:     newMsg(nil, map[string]any{"kafka_tombstone_message": true}),
			wantNil: true,
		},
		{
			name: "payload is dropped for tombstones",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:     newMsg([]byte("null"), map[string]any{"kafka_tombstone_message": true}),
			wantNil: true,
		},
		{
			name: "non-tombstone keeps payload",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:       newMsg([]byte("hello"), map[string]any{"kafka_tombstone_message": false}),
			wantValue: []byte("hello"),
		},
		{
			name: "missing metadata with or fallback is not a tombstone",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message.or(false) }'
`,
			msg:       newMsg([]byte(""), nil),
			wantValue: []byte{},
		},
		{
			name: "content based mapping",
			conf: `
topic: foo
tombstone: '${! this.op == "delete" }'
`,
			msg:     newMsg([]byte(`{"op":"delete"}`), nil),
			wantNil: true,
		},
		{
			name: "unparseable value fails the write",
			conf: `
topic: foo
tombstone: '${! @kafka_tombstone_message }'
`,
			msg:         newMsg([]byte("hello"), nil),
			errContains: "parse tombstone",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(tc.conf, nil)
			require.NoError(t, err)

			w, err := NewFranzWriterFromConfig(pConf, franzWriterHooks{})
			require.NoError(t, err)

			records, err := w.messageBatchToFranzRecords(service.MessageBatch{tc.msg})
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
			require.Len(t, records, 1)

			if tc.wantNil {
				assert.Nil(t, records[0].Value)
				return
			}
			require.NotNil(t, records[0].Value)
			assert.Equal(t, tc.wantValue, records[0].Value)
		})
	}
}
