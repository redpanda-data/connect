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

package migrator_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

func TestMigratorSchemaIDTranslationDoesNotMutateInput(t *testing.T) {
	const (
		srcID = 263
		dstID = 292
	)

	t.Run("input message not mutated", func(t *testing.T) {
		m := migrator.NewMigratorForSchemaIDTranslationTesting(t, true, map[int]int{srcID: dstID}, "foo")
		msg := schemaIDTestMessage(srcID, "payload")

		records, err := m.MessageBatchToFranzRecords(service.MessageBatch{msg})
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, dstID, schemaIDOf(t, records[0].Value))

		assert.Equal(t, srcID, messageSchemaID(t, msg), "input message must keep the source schema ID")
	})

	t.Run("retry is idempotent", func(t *testing.T) {
		m := migrator.NewMigratorForSchemaIDTranslationTesting(t, true, map[int]int{srcID: dstID}, "foo")
		batch := service.MessageBatch{schemaIDTestMessage(srcID, "payload")}

		for attempt := 1; attempt <= 2; attempt++ {
			records, err := m.MessageBatchToFranzRecords(batch)
			require.NoError(t, err, "attempt %d", attempt)
			require.Len(t, records, 1)
			assert.Equal(t, dstID, schemaIDOf(t, records[0].Value), "attempt %d", attempt)
		}
	})

	t.Run("retry does not silently translate to the wrong ID", func(t *testing.T) {
		const otherDstID = 310
		// 292 is also a valid source ID, mapped to a different destination.
		m := migrator.NewMigratorForSchemaIDTranslationTesting(t, true, map[int]int{srcID: dstID, dstID: otherDstID}, "foo")
		batch := service.MessageBatch{schemaIDTestMessage(srcID, "payload")}

		for attempt := 1; attempt <= 2; attempt++ {
			records, err := m.MessageBatchToFranzRecords(batch)
			require.NoError(t, err, "attempt %d", attempt)
			require.Len(t, records, 1)
			assert.Equal(t, dstID, schemaIDOf(t, records[0].Value), "attempt %d", attempt)
		}
	})

	t.Run("partial batch failure leaves earlier messages untouched", func(t *testing.T) {
		const unknownID = 999
		m := migrator.NewMigratorForSchemaIDTranslationTesting(t, true, map[int]int{srcID: dstID}, "foo")
		first := schemaIDTestMessage(srcID, "one")
		second := schemaIDTestMessage(unknownID, "two")

		_, err := m.MessageBatchToFranzRecords(service.MessageBatch{first, second})
		require.ErrorContains(t, err, "schema ID 999 not found in registry")

		assert.Equal(t, srcID, messageSchemaID(t, first), "first message was rewritten although the batch failed")
	})
}

func confluentValue(id int, payload string) []byte {
	b := []byte{0x00, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(id))
	return append(b, payload...)
}

func schemaIDOf(t *testing.T, b []byte) int {
	t.Helper()
	require.GreaterOrEqual(t, len(b), 5)
	return int(binary.BigEndian.Uint32(b[1:5]))
}

func schemaIDTestMessage(id int, payload string) *service.Message {
	msg := service.NewMessage(confluentValue(id, payload))
	msg.MetaSetMut("kafka_topic", "foo")
	msg.MetaSetMut("kafka_partition", 0)
	msg.MetaSetMut("kafka_timestamp_ms", int64(1))
	return msg
}

func messageSchemaID(t *testing.T, msg *service.Message) int {
	t.Helper()
	b, err := msg.AsBytes()
	require.NoError(t, err)
	return schemaIDOf(t, b)
}
