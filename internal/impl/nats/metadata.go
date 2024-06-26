// Copyright 2024 Redpanda Data, Inc.
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
	"github.com/nats-io/nats.go/jetstream"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	metaKVKey       = "nats_kv_key"
	metaKVBucket    = "nats_kv_bucket"
	metaKVRevision  = "nats_kv_revision"
	metaKVDelta     = "nats_kv_delta"
	metaKVOperation = "nats_kv_operation"
	metaKVCreated   = "nats_kv_created"
)

func newMessageFromKVEntry(entry jetstream.KeyValueEntry) *service.Message {
	msg := service.NewMessage(entry.Value())
	msg.MetaSetMut(metaKVKey, entry.Key())
	msg.MetaSetMut(metaKVBucket, entry.Bucket())
	msg.MetaSetMut(metaKVRevision, entry.Revision())
	msg.MetaSetMut(metaKVDelta, entry.Delta())
	msg.MetaSetMut(metaKVOperation, entry.Operation().String())
	msg.MetaSetMut(metaKVCreated, entry.Created())

	return msg
}
