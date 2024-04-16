package nats

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	kvpFieldOperation = "operation"
	kvpFieldKey       = "key"
	kvpFieldRevision  = "revision"
	kvpFieldTimeout   = "timeout"
)

type kvpOperationType string

const (
	kvpOperationGet         kvpOperationType = "get"
	kvpOperationGetRevision kvpOperationType = "get_revision"
	kvpOperationCreate      kvpOperationType = "create"
	kvpOperationPut         kvpOperationType = "put"
	kvpOperationUpdate      kvpOperationType = "update"
	kvpOperationDelete      kvpOperationType = "delete"
	kvpOperationPurge       kvpOperationType = "purge"
	kvpOperationHistory     kvpOperationType = "history"
	kvpOperationKeys        kvpOperationType = "keys"
)

var kvpOperations = map[string]string{
	string(kvpOperationGet):         "Returns the latest value for `key`.",
	string(kvpOperationGetRevision): "Returns the value of `key` for the specified `revision`.",
	string(kvpOperationCreate):      "Adds the key/value pair if it does not exist. Returns an error if it already exists.",
	string(kvpOperationPut):         "Places a new value for the key into the store.",
	string(kvpOperationUpdate):      "Updates the value for `key` only if the `revision` matches the latest revision.",
	string(kvpOperationDelete):      "Deletes the key/value pair, but keeps historical values.",
	string(kvpOperationPurge):       "Deletes the key/value pair and all historical values.",
	string(kvpOperationHistory):     "Returns historical values of `key` as an array of objects containing the following fields: `key`, `value`, `bucket`, `revision`, `delta`, `operation`, `created`.",
	string(kvpOperationKeys):        "Returns the keys in the `bucket` which match the `keys_filter` as an array of strings.",
}

func natsKVProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.12.0").
		Summary("Perform operations on a NATS key-value bucket.").
		Description(`
### KV Operations

The NATS KV processor supports a multitude of KV operations via the [operation](#operation) field. Along with ` + "`get`" + `, ` + "`put`" + `, and ` + "`delete`" + `, this processor supports atomic operations like ` + "`update`" + ` and ` + "`create`" + `, as well as utility operations like ` + "`purge`" + `, ` + "`history`" + `, and ` + "`keys`" + `.

### Metadata

This processor adds the following metadata fields to each message, depending on the chosen ` + "`operation`" + `:

#### get, get_revision
` + "``` text" + `
- nats_kv_key
- nats_kv_bucket
- nats_kv_revision
- nats_kv_delta
- nats_kv_operation
- nats_kv_created
` + "```" + `

#### create, update, delete, purge
` + "``` text" + `
- nats_kv_key
- nats_kv_bucket
- nats_kv_revision
- nats_kv_operation
` + "```" + `

#### keys
` + "``` text" + `
- nats_kv_bucket
` + "```" + `

` + connectionNameDescription() + authDescription()).
		Fields(kvDocs([]*service.ConfigField{
			service.NewStringAnnotatedEnumField(kvpFieldOperation, kvpOperations).
				Description("The operation to perform on the KV bucket."),
			service.NewInterpolatedStringField(kvpFieldKey).
				Description("The key for each message. Supports [wildcards](https://docs.nats.io/nats-concepts/subjects#wildcards) for the `history` and `keys` operations.").
				Example("foo").
				Example("foo.bar.baz").
				Example("foo.*").
				Example("foo.>").
				Example(`foo.${! json("meta.type") }`).LintRule(`if this == "" {[ "'key' must be set to a non-empty string" ]}`),
			service.NewInterpolatedStringField(kvpFieldRevision).
				Description("The revision of the key to operate on. Used for `get_revision` and `update` operations.").
				Example("42").
				Example(`${! @nats_kv_revision }`).
				Optional().
				Advanced(),
			service.NewDurationField(kvpFieldTimeout).
				Description("The maximum period to wait on an operation before aborting and returning an error.").
				Advanced().Default("5s"),
		}...)...).
		LintRule(`root = match {
      ["get_revision", "update"].contains(this.operation) && !this.exists("revision") => [ "'revision' must be set when operation is '" + this.operation + "'" ],
      !["get_revision", "update"].contains(this.operation) && this.exists("revision") => [ "'revision' cannot be set when operation is '" + this.operation + "'" ],
    }`)
}

func init() {
	err := service.RegisterProcessor(
		"nats_kv", natsKVProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newKVProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvProcessor struct {
	connDetails connectionDetails
	bucket      string
	operation   kvpOperationType
	key         *service.InterpolatedString
	revision    *service.InterpolatedString
	timeout     time.Duration

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	kv       jetstream.KeyValue
}

func newKVProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*kvProcessor, error) {
	p := &kvProcessor{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if p.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if p.bucket, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	if operation, err := conf.FieldString(kvpFieldOperation); err != nil {
		return nil, err
	} else {
		p.operation = kvpOperationType(operation)
	}

	if p.key, err = conf.FieldInterpolatedString(kvpFieldKey); err != nil {
		return nil, err
	}

	if conf.Contains(kvpFieldRevision) {
		if p.revision, err = conf.FieldInterpolatedString(kvpFieldRevision); err != nil {
			return nil, err
		}
	}

	if p.timeout, err = conf.FieldDuration(kvpFieldTimeout); err != nil {
		return nil, err
	}

	err = p.Connect(context.Background())
	return p, err
}

func (p *kvProcessor) disconnect() {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		p.natsConn.Close()
		p.natsConn = nil
	}
	p.kv = nil
}

func (p *kvProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	p.connMut.Lock()
	kv := p.kv
	p.connMut.Unlock()

	key, err := p.key.TryString(msg)
	if err != nil {
		return nil, err
	}

	bytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithTimeout(ctx, p.timeout)
	defer done()

	switch p.operation {

	case kvpOperationGet:
		entry, err := kv.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

	case kvpOperationGetRevision:
		revision, err := p.parseRevision(msg)
		if err != nil {
			return nil, err
		}
		entry, err := kv.GetRevision(ctx, key, revision)
		if err != nil {
			return nil, err
		}
		return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

	case kvpOperationCreate:
		revision, err := kv.Create(ctx, key, bytes)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvpOperationPut:
		revision, err := kv.Put(ctx, key, bytes)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvpOperationUpdate:
		revision, err := p.parseRevision(msg)
		if err != nil {
			return nil, err
		}
		revision, err = kv.Update(ctx, key, bytes, revision)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvpOperationDelete:
		// TODO: Support revision here?
		err := kv.Delete(ctx, key)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, 0, nats.KeyValueDelete)
		return service.MessageBatch{m}, nil

	case kvpOperationPurge:
		err := kv.Purge(ctx, key)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, 0, nats.KeyValuePurge)
		return service.MessageBatch{m}, nil

	case kvpOperationHistory:
		entries, err := kv.History(ctx, key)
		if err != nil {
			return nil, err
		}
		var records []any
		for _, entry := range entries {
			records = append(records, map[string]any{
				"key":       entry.Key(),
				"value":     entry.Value(),
				"bucket":    entry.Bucket(),
				"revision":  entry.Revision(),
				"delta":     entry.Delta(),
				"operation": entry.Operation().String(),
				"created":   entry.Created(),
			})
		}

		m := service.NewMessage(nil)
		m.SetStructuredMut(records)
		return service.MessageBatch{m}, nil

	case kvpOperationKeys:
		// `kv.ListKeys()` does not allow users to specify a key filter, so we call `kv.Watch()` directly.
		watcher, err := kv.Watch(ctx, key, []jetstream.WatchOpt{jetstream.IgnoreDeletes(), jetstream.MetaOnly()}...)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := watcher.Stop(); err != nil {
				p.log.Debugf("Failed to close key watcher: %s", err)
			}
		}()

		var keys []any
	loop:
		for {
			select {
			case entry := <-watcher.Updates():
				if entry == nil {
					break loop
				}
				keys = append(keys, entry.Key())
			case <-ctx.Done():
				return nil, fmt.Errorf("watcher update loop exited prematurely: %s", ctx.Err())
			}
		}

		m := service.NewMessage(nil)
		m.SetStructuredMut(keys)
		m.MetaSetMut(metaKVBucket, p.bucket)
		return service.MessageBatch{m}, nil

	default:
		return nil, fmt.Errorf("invalid kv operation: %s", p.operation)
	}
}

func (p *kvProcessor) parseRevision(msg *service.Message) (uint64, error) {
	revStr, err := p.revision.TryString(msg)
	if err != nil {
		return 0, err
	}

	return strconv.ParseUint(revStr, 10, 64)
}

func (p *kvProcessor) addMetadata(msg *service.Message, key string, revision uint64, operation nats.KeyValueOp) {
	msg.MetaSetMut(metaKVKey, key)
	msg.MetaSetMut(metaKVBucket, p.bucket)
	msg.MetaSetMut(metaKVRevision, revision)
	msg.MetaSetMut(metaKVOperation, operation.String())
}

func (p *kvProcessor) Connect(ctx context.Context) (err error) {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		return nil
	}

	defer func() {
		if err != nil {
			if p.natsConn != nil {
				p.natsConn.Close()
			}
		}
	}()

	if p.natsConn, err = p.connDetails.get(ctx); err != nil {
		return err
	}

	js, err := jetstream.New(p.natsConn)
	if err != nil {
		return err
	}

	p.kv, err = js.KeyValue(ctx, p.bucket)
	if err != nil {
		return err
	}
	return nil
}

func (p *kvProcessor) Close(ctx context.Context) error {
	go func() {
		p.disconnect()
		p.shutSig.ShutdownComplete()
	}()
	select {
	case <-p.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
