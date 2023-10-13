package nats

import (
	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	metaKVKey       = "nats_kv_key"
	metaKVBucket    = "nats_kv_bucket"
	metaKVRevision  = "nats_kv_revision"
	metaKVDelta     = "nats_kv_delta"
	metaKVOperation = "nats_kv_operation"
	metaKVCreated   = "nats_kv_created"

	tracingVersion = "4.23.0"
)

func newMessageFromKVEntry(entry nats.KeyValueEntry) *service.Message {
	msg := service.NewMessage(entry.Value())
	msg.MetaSetMut(metaKVKey, entry.Key())
	msg.MetaSetMut(metaKVBucket, entry.Bucket())
	msg.MetaSetMut(metaKVRevision, entry.Revision())
	msg.MetaSetMut(metaKVDelta, entry.Delta())
	msg.MetaSetMut(metaKVOperation, entry.Operation().String())
	msg.MetaSetMut(metaKVCreated, entry.Created())

	return msg
}

func ConnectionNameDescription() string {
	return `### Connection Name

When monitoring and managing a production NATS system, it is often useful to
know which connection a message was send/received from. This can be achieved by
setting the connection name option when creating a NATS connection.

Benthos will automatically set the connection name based off the label of the given
NATS component, so that monitoring tools between NATS and benthos can stay in sync.
`
}
