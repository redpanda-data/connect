// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

func newFranzReaderOrdered(pConf *service.ParsedConfig, mgr *service.Resources) (*kafka.FranzReaderOrdered, error) {
	var opts []kgo.Opt

	connOpts, err := kafka.FranzConnectionOptsFromConfig(pConf, mgr.Logger())
	if err != nil {
		return nil, err
	}
	opts = append(opts, connOpts...)

	consumerOpts, err := kafka.FranzConsumerOptsFromConfig(pConf)
	if err != nil {
		return nil, err
	}
	opts = append(opts, consumerOpts...)

	fr, err := kafka.NewFranzReaderOrderedFromConfig(pConf, mgr,
		func() ([]kgo.Opt, error) {
			return opts, nil
		})
	if err != nil {
		return nil, err
	}

	return fr, nil
}

// lazyFranzSharedClientInfo defers client creation until Connect due to
// API restrictions.
type lazyFranzSharedClientInfo struct {
	opts []kgo.Opt
	conn *kafka.FranzConnectionDetails
	ptr  atomic.Pointer[kafka.FranzSharedClientInfo]
	mu   sync.Mutex
}

func (l *lazyFranzSharedClientInfo) GetClient(ctx context.Context) (*kafka.FranzSharedClientInfo, error) {
	if ptr := l.ptr.Load(); ptr != nil {
		return ptr, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Check again after obtaining the lock to avoid a race
	if ptr := l.ptr.Load(); ptr != nil {
		return ptr, nil
	}

	client, err := kafka.NewFranzClient(ctx, l.opts...)
	if err != nil {
		return nil, err
	}

	v := &kafka.FranzSharedClientInfo{
		Client:      client,
		ConnDetails: l.conn,
	}
	l.ptr.Store(v)
	return v, nil
}

func (l *lazyFranzSharedClientInfo) Close(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if ptr := l.ptr.Load(); ptr != nil {
		ptr.Client.Close()
		l.ptr.Store(nil)
	}

	return nil
}

// franzWriter wraps a FranzWriter to allow getting the client from the hooks.
type franzWriter struct {
	*kafka.FranzWriter
	lazy *lazyFranzSharedClientInfo
}

func (fw franzWriter) GetClient(ctx context.Context) (*kafka.FranzSharedClientInfo, error) {
	return fw.lazy.GetClient(ctx)
}

func newFranzWriter(pConf *service.ParsedConfig, mgr *service.Resources) (franzWriter, error) {
	connDetails, err := kafka.FranzConnectionDetailsFromConfig(pConf, mgr.Logger())
	if err != nil {
		return franzWriter{}, err
	}

	var opts []kgo.Opt
	opts = append(opts, connDetails.FranzOpts()...)

	producerOpts, err := kafka.FranzProducerOptsFromConfig(pConf)
	if err != nil {
		return franzWriter{}, err
	}
	opts = append(opts, producerOpts...)
	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	lazy := lazyFranzSharedClientInfo{
		opts: opts,
		conn: connDetails,
	}
	hooks := kafka.NewFranzWriterHooks(func(ctx context.Context, fn kafka.FranzSharedClientUseFn) error {
		client, err := lazy.GetClient(ctx)
		if err != nil {
			return err
		}
		return fn(client)
	}).WithYieldClientFn(lazy.Close)

	fw, err := kafka.NewFranzWriterFromConfig(pConf, hooks)
	if err != nil {
		return franzWriter{}, err
	}

	// Partition and timestamp are mandatory fields that are passed as metadata.
	// They must not be changed by the migrator otherwise consumer group
	// migration will break.
	if fw.Key != nil {
		return franzWriter{}, errors.New("key field is not supported by migrator, setting it could break consumer group migration")
	}
	if fw.Partition != nil {
		return franzWriter{}, errors.New("partition field is not supported by migrator, setting it could break consumer group migration")
	}
	if fw.Timestamp != nil {
		return franzWriter{}, errors.New("timestamp and timestamp_ms fields are not supported by migrator, setting it could break consumer group migration")
	}
	fw.IsTimestampMs = true

	return franzWriter{fw, &lazy}, nil
}
