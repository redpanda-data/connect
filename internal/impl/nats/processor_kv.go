package nats

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	kvGet         = "get"
	kvGetRevision = "get_revision"
	kvCreate      = "create"
	kvPut         = "put"
	kvUpdate      = "update"
	kvDelete      = "delete"
	kvPurge       = "purge"
	kvHistory     = "history"
	kvKeys        = "keys"
)

var kvOps = map[string]string{
	kvGet:         "Returns the latest value for `key`.",
	kvGetRevision: "Returns the value of `key` for the specified `revision`.",
	kvCreate:      "Adds the key/value pair if it does not exist. Returns an error if it already exists.",
	kvPut:         "Places a new value for the key into the store.",
	kvUpdate:      "Updates the value for `key` only if the `revision` matches the latest revision.",
	kvDelete:      "Deletes the key/value pair, but keeps historical values.",
	kvPurge:       "Deletes the key/value pair and all historical values.",
	kvHistory:     "Returns historical values of `key` as a batch.",
	kvKeys:        "Returns all the keys in the `bucket` as a batch.",
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

#### get, get_revision, history
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

` + ConnectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringField("bucket").
			Description("The name of the KV bucket to watch for updates.").
			Example("my_kv_bucket")).
		Field(service.NewStringAnnotatedEnumField("operation", kvOps).
			Description(`The operation to perform on the KV bucket.
`)).
		Field(service.NewInterpolatedStringField("key").
			Description("The key for each message.").
			Default("").
			Example("foo").
			Example("foo.bar.baz").
			Example(`foo.${! json("meta.type") }`)).
		Field(service.NewInterpolatedStringField("revision").
			Description("The revision of the key to operate on. Used for `get_revision` and `update` operations.").
			Example("42").
			Example(`${! @nats_kv_revision }`).
			Optional().
			Advanced()).
		Fields(connectionTailFields()...).
		LintRule(`root = match {
      ["get_revision", "update"].contains(this.operation) && !this.exists("revision") => [ "'revision' must be set when operation is '" + this.operation + "'" ],
      !["get_revision", "update"].contains(this.operation) && this.exists("revision") => [ "'revision' cannot be set when operation is '" + this.operation + "'" ],
      this.key == "" && this.operation != "keys" => [ "'key' must be set when operation is '" + this.operation + "'" ],
      this.key != "" && this.operation == "keys" => [ "'key' cannot be set when operation is '" + this.operation + "'" ],
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
	operation   string
	key         *service.InterpolatedString
	keyRaw      string
	revision    *service.InterpolatedString
	revisionRaw string

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	kv       nats.KeyValue
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

	if p.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	if p.operation, err = conf.FieldString("operation"); err != nil {
		return nil, err
	}

	if p.keyRaw, err = conf.FieldString("key"); err != nil {
		return nil, err
	}

	if p.key, err = conf.FieldInterpolatedString("key"); err != nil {
		return nil, err
	}

	if conf.Contains("revision") {
		if p.revisionRaw, err = conf.FieldString("revision"); err != nil {
			return nil, err
		}

		if p.revision, err = conf.FieldInterpolatedString("revision"); err != nil {
			return nil, err
		}
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

func (p *kvProcessor) Process(c context.Context, msg *service.Message) (service.MessageBatch, error) {
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

	switch p.operation {

	case kvGet:
		entry, err := kv.Get(key)
		if err != nil {
			return nil, err
		}
		return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

	case kvGetRevision:
		revision, err := p.parseRevision(msg)
		if err != nil {
			return nil, err
		}
		entry, err := kv.GetRevision(key, revision)
		if err != nil {
			return nil, err
		}
		return service.MessageBatch{newMessageFromKVEntry(entry)}, nil

	case kvCreate:
		revision, err := kv.Create(key, bytes)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvPut:
		revision, err := kv.Put(key, bytes)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvUpdate:
		revision, err := p.parseRevision(msg)
		if err != nil {
			return nil, err
		}
		revision, err = kv.Update(key, bytes, revision)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, revision, nats.KeyValuePut)
		return service.MessageBatch{m}, nil

	case kvDelete:
		// TODO: Support revision here?
		err := kv.Delete(key)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, 0, nats.KeyValueDelete)
		return service.MessageBatch{m}, nil

	case kvPurge:
		err := kv.Purge(key)
		if err != nil {
			return nil, err
		}

		m := msg.Copy()
		p.addMetadata(m, key, 0, nats.KeyValuePurge)
		return service.MessageBatch{m}, nil

	case kvHistory:
		entries, err := kv.History(key)
		if err != nil {
			return nil, err
		}
		batch := service.MessageBatch{}
		for _, entry := range entries {
			batch = append(batch, newMessageFromKVEntry(entry))
		}
		return batch, nil

	case kvKeys:
		keys, err := kv.Keys()
		if err != nil {
			return nil, err
		}
		batch := service.MessageBatch{}
		for _, key := range keys {
			m := service.NewMessage([]byte(key))
			m.MetaSetMut(metaKVBucket, p.bucket)
			batch = append(batch, m)
		}
		return batch, nil

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

	js, err := p.natsConn.JetStream()
	if err != nil {
		return err
	}

	p.kv, err = js.KeyValue(p.bucket)
	if err != nil {
		return err
	}

	p.log.Infof("Performing operation `%s` on NATS KV bucket: %s", p.operation, p.bucket)
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
