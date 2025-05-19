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
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	kvoFieldKey = "key"
)

func natsKVOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.12.0").
		Summary("Put messages in a NATS key-value bucket.").
		Description(`
The field ` + "`key`" + ` supports
xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing
you to create a unique key for each message.

` + connectionNameDescription() + authDescription()).
		Fields(kvDocs([]*service.ConfigField{
			service.NewInterpolatedStringField(kvoFieldKey).
				Description("The key for each message.").
				Example("foo").
				Example("foo.bar.baz").
				Example(`foo.${! json("meta.type") }`),
			service.NewOutputMaxInFlightField().Default(1024),
		}...)...)
}

func init() {
	service.MustRegisterOutput(
		"nats_kv", natsKVOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newKVOutput(conf, mgr)
			return w, maxInFlight, err
		})
}

//------------------------------------------------------------------------------

type kvOutput struct {
	connDetails connectionDetails
	bucket      string
	key         *service.InterpolatedString
	keyRaw      string

	log *service.Logger

	connMut  sync.Mutex
	natsConn *nats.Conn
	keyValue jetstream.KeyValue

	shutSig *shutdown.Signaller
}

func newKVOutput(conf *service.ParsedConfig, mgr *service.Resources) (*kvOutput, error) {
	kv := kvOutput{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if kv.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if kv.bucket, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	if kv.keyRaw, err = conf.FieldString(kvoFieldKey); err != nil {
		return nil, err
	}

	if kv.key, err = conf.FieldInterpolatedString(kvoFieldKey); err != nil {
		return nil, err
	}
	return &kv, nil
}

//------------------------------------------------------------------------------

func (kv *kvOutput) Connect(ctx context.Context) (err error) {
	kv.connMut.Lock()
	defer kv.connMut.Unlock()

	if kv.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn

	defer func() {
		if err != nil && natsConn != nil {
			natsConn.Close()
		}
	}()

	if natsConn, err = kv.connDetails.get(ctx); err != nil {
		return err
	}

	jsc, err := jetstream.New(natsConn)
	if err != nil {
		return err
	}

	kv.keyValue, err = jsc.KeyValue(ctx, kv.bucket)
	if err != nil {
		return err
	}

	kv.natsConn = natsConn
	return nil
}

func (kv *kvOutput) disconnect() {
	kv.connMut.Lock()
	defer kv.connMut.Unlock()

	if kv.natsConn != nil {
		kv.natsConn.Close()
		kv.natsConn = nil
	}
	kv.keyValue = nil
}

//------------------------------------------------------------------------------

func (kv *kvOutput) Write(ctx context.Context, msg *service.Message) error {
	kv.connMut.Lock()
	keyValue := kv.keyValue
	kv.connMut.Unlock()
	if keyValue == nil {
		return service.ErrNotConnected
	}

	value, err := msg.AsBytes()
	if err != nil {
		return err
	}

	key, err := kv.key.TryString(msg)
	if err != nil {
		return err
	}

	rev, err := keyValue.Put(ctx, key, value)
	if err != nil {
		return err
	}

	kv.log.With(
		metaKVBucket, keyValue.Bucket(),
		metaKVKey, key,
		metaKVRevision, rev,
	).Debug("Updated kv bucket entry")

	return nil
}

func (kv *kvOutput) Close(ctx context.Context) error {
	go func() {
		kv.disconnect()
		kv.shutSig.TriggerHasStopped()
	}()
	select {
	case <-kv.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
