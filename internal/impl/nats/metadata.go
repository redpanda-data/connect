package nats

import (
	"github.com/nats-io/nats.go/jetstream"

	"github.com/benthosdev/benthos/v4/public/service"
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
