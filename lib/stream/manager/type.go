// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package manager

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/stream"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// streamWrapper tracks a stream along with information regarding its internals.
type streamWrapper struct {
	strm      *stream.Type
	config    stream.Config
	running   uint32
	createdAt time.Time
}

func (s *streamWrapper) Running() bool {
	return atomic.LoadUint32(&s.running) == 1
}

//------------------------------------------------------------------------------

// Type manages a collection of streams, providing APIs for CRUD operations on
// the streams.
type Type struct {
	streams map[string]*streamWrapper

	manager types.Manager
	stats   metrics.Type
	logger  log.Modular

	lock sync.Mutex
}

// New creates a new stream manager.Type.
func New(opts ...func(*Type)) *Type {
	t := &Type{
		streams: map[string]*streamWrapper{},
		manager: types.DudMgr{},
		stats:   metrics.DudType{},
		logger:  log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

//------------------------------------------------------------------------------

// OptSetStats sets the metrics aggregator to be used by the manager and all
// child streams.
func OptSetStats(stats metrics.Type) func(*Type) {
	return func(t *Type) {
		t.stats = stats
	}
}

// OptSetLogger sets the logging output to be used by the manager and all child
// streams.
func OptSetLogger(log log.Modular) func(*Type) {
	return func(t *Type) {
		t.logger = log
	}
}

// OptSetManager sets the service manager to be used by the stream manager and
// all child streams.
func OptSetManager(mgr types.Manager) func(*Type) {
	return func(t *Type) {
		t.manager = mgr
	}
}

//------------------------------------------------------------------------------

// Errors specifically returned by a stream manager.
var (
	ErrStreamExists       = errors.New("stream already exists")
	ErrStreamDoesNotExist = errors.New("stream does not exist")
)

//------------------------------------------------------------------------------

// Create attempts to construct and run a new stream under a unique ID. If the
// ID already exists an error is returned.
func (m *Type) Create(id string, conf stream.Config) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, exists := m.streams[id]; exists {
		return ErrStreamExists
	}

	wrapper := &streamWrapper{
		running:   1,
		config:    conf,
		createdAt: time.Now(),
	}

	var err error
	if wrapper.strm, err = stream.New(
		conf,
		stream.OptSetLogger(m.logger.NewModule("."+id)),
		stream.OptSetStats(metrics.Namespaced(m.stats, id)),
		stream.OptSetManager(m.manager),
		stream.OptOnClose(func() {
			atomic.StoreUint32(&wrapper.running, 0)
		}),
	); err != nil {
		return err
	}

	m.streams[id] = wrapper
	return nil
}

// StreamStatus contains fields used to describe the current status of a managed
// stream.
type StreamStatus struct {
	Active bool
	Uptime time.Duration
	Config stream.Config
}

// Read attempts to obtain the status of a managed stream. Returns an error if
// the stream does not exist.
func (m *Type) Read(id string) (StreamStatus, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	status := StreamStatus{}
	wrapper, exists := m.streams[id]
	if !exists {
		return status, ErrStreamDoesNotExist
	}

	status.Active = wrapper.Running()
	status.Config = wrapper.config
	status.Uptime = time.Since(wrapper.createdAt)

	return status, nil
}

// Update attempts to stop an existing stream and replace it with a new version
// of the same stream.
func (m *Type) Update(id string, conf stream.Config, timeout time.Duration) error {
	m.lock.Lock()
	wrapper, exists := m.streams[id]
	m.lock.Unlock()
	if !exists {
		return ErrStreamDoesNotExist
	}

	if reflect.DeepEqual(wrapper.config, conf) {
		return nil
	}

	if err := m.Delete(id, timeout); err != nil {
		return err
	}
	return m.Create(id, conf)
}

// Delete attempts to stop and remove a stream by its ID. Returns an error if
// the stream was not found, or if clean shutdown fails in the specified period
// of time.
func (m *Type) Delete(id string, timeout time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	wrapper, exists := m.streams[id]
	if !exists {
		return ErrStreamDoesNotExist
	}

	if err := wrapper.strm.Stop(timeout); err != nil {
		return err
	}
	delete(m.streams, id)

	return nil
}

//------------------------------------------------------------------------------
