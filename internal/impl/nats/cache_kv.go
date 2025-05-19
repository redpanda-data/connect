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
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func natsKVCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.27.0").
		Summary("Cache key/values in a NATS key-value bucket.").
		Description(connectionNameDescription() + authDescription()).
		Fields(kvDocs()...)
}

func init() {
	service.MustRegisterCache(
		"nats_kv", natsKVCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newKVCache(conf, mgr)
		},
	)
}

type kvCache struct {
	connDetails connectionDetails
	bucket      string

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.RWMutex
	natsConn *nats.Conn
	kv       jetstream.KeyValue
}

func newKVCache(conf *service.ParsedConfig, mgr *service.Resources) (*kvCache, error) {
	p := &kvCache{
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

	err = p.connect(context.Background())
	return p, err
}

func (p *kvCache) disconnect() {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		p.natsConn.Close()
		p.natsConn = nil
	}
	p.kv = nil
}

func (p *kvCache) connect(ctx context.Context) error {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		return nil
	}

	var err error
	if p.natsConn, err = p.connDetails.get(ctx); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			p.natsConn.Close()
			p.natsConn = nil
		}
	}()

	var js jetstream.JetStream
	if js, err = jetstream.New(p.natsConn); err != nil {
		return err
	}

	if p.kv, err = js.KeyValue(ctx, p.bucket); err != nil {
		return err
	}
	return nil
}

func (p *kvCache) Get(ctx context.Context, key string) ([]byte, error) {
	p.connMut.RLock()
	defer p.connMut.RUnlock()

	entry, err := p.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			err = service.ErrKeyNotFound
		}
		return nil, err
	}
	return entry.Value(), nil
}

func (p *kvCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()

	_, err := p.kv.Put(ctx, key, value)
	return err
}

func (p *kvCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()
	_, err := p.kv.Create(ctx, key, value)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return service.ErrKeyAlreadyExists
	}
	return err
}

func (p *kvCache) Delete(ctx context.Context, key string) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()
	return p.kv.Delete(ctx, key)
}

func (p *kvCache) Close(ctx context.Context) error {
	go func() {
		p.disconnect()
		p.shutSig.TriggerHasStopped()
	}()
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
