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
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// StreamStatus tracks a stream along with information regarding its internals.
type StreamStatus struct {
	stoppedAfter int64
	config       stream.Config
	strm         *stream.Type
	logger       log.Modular
	metrics      *metrics.Local
	createdAt    time.Time
}

// NewStreamStatus creates a new StreamStatus.
func NewStreamStatus(
	conf stream.Config,
	strm *stream.Type,
	logger log.Modular,
	stats *metrics.Local,
) *StreamStatus {
	return &StreamStatus{
		config:    conf,
		strm:      strm,
		logger:    logger,
		metrics:   stats,
		createdAt: time.Now(),
	}
}

// IsRunning returns a boolean indicating whether the stream is currently
// running.
func (s *StreamStatus) IsRunning() bool {
	return atomic.LoadInt64(&s.stoppedAfter) == 0
}

// Uptime returns a time.Duration indicating the current uptime of the stream.
func (s *StreamStatus) Uptime() time.Duration {
	if stoppedAfter := atomic.LoadInt64(&s.stoppedAfter); stoppedAfter > 0 {
		return time.Duration(stoppedAfter)
	}
	return time.Since(s.createdAt)
}

// Config returns the configuration of the stream.
func (s *StreamStatus) Config() stream.Config {
	return s.config
}

// Metrics returns a metrics aggregator of the stream.
func (s *StreamStatus) Metrics() *metrics.Local {
	return s.metrics
}

// Logger returns the logger of the stream.
func (s *StreamStatus) Logger() log.Modular {
	return s.logger
}

// setClosed sets the flag indicating that the stream is closed.
func (s *StreamStatus) setClosed() {
	atomic.SwapInt64(&s.stoppedAfter, int64(time.Since(s.createdAt)))
}

//------------------------------------------------------------------------------

// StreamProcConstructorFunc is a closure type that constructs a processor type
// for new streams, where the id of the stream is provided as an argument.
type StreamProcConstructorFunc func(streamID string) (types.Processor, error)

//------------------------------------------------------------------------------

// Type manages a collection of streams, providing APIs for CRUD operations on
// the streams.
type Type struct {
	closed  bool
	streams map[string]*StreamStatus

	manager    types.Manager
	stats      metrics.Type
	logger     log.Modular
	apiTimeout time.Duration

	pipelineProcCtors []StreamProcConstructorFunc

	lock sync.Mutex
}

// New creates a new stream manager.Type.
func New(opts ...func(*Type)) *Type {
	t := &Type{
		streams:    map[string]*StreamStatus{},
		manager:    types.DudMgr{},
		stats:      metrics.DudType{},
		apiTimeout: time.Second * 5,
		logger:     log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
	}
	for _, opt := range opts {
		opt(t)
	}
	t.registerEndpoints()
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

// OptSetAPITimeout sets the default timeout for HTTP API requests.
func OptSetAPITimeout(tout time.Duration) func(*Type) {
	return func(t *Type) {
		t.apiTimeout = tout
	}
}

// OptAddProcessors adds processor constructors that will be called for every
// new stream and attached to the processor pipelines. The constructor is given
// the name of the stream as an argument.
func OptAddProcessors(procs ...StreamProcConstructorFunc) func(*Type) {
	return func(t *Type) {
		t.pipelineProcCtors = append(t.pipelineProcCtors, procs...)
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

	if m.closed {
		return types.ErrTypeClosed
	}

	if _, exists := m.streams[id]; exists {
		return ErrStreamExists
	}

	var procCtors []types.ProcessorConstructorFunc
	for _, ctor := range m.pipelineProcCtors {
		func(c StreamProcConstructorFunc) {
			procCtors = append(procCtors, func() (types.Processor, error) {
				return c(id)
			})
		}(ctor)
	}

	strmLogger := m.logger.NewModule("." + id)
	strmFlatMetrics := metrics.NewLocal()

	var wrapper *StreamStatus
	strm, err := stream.New(
		conf,
		stream.OptAddProcessors(procCtors...),
		stream.OptSetLogger(strmLogger),
		stream.OptSetStats(metrics.Combine(metrics.Namespaced(m.stats, id), strmFlatMetrics)),
		stream.OptSetManager(namespacedMgr(id, m.manager)),
		stream.OptOnClose(func() {
			wrapper.setClosed()
		}),
	)
	if err != nil {
		return err
	}

	wrapper = NewStreamStatus(conf, strm, strmLogger, strmFlatMetrics)
	m.streams[id] = wrapper
	return nil
}

// Read attempts to obtain the status of a managed stream. Returns an error if
// the stream does not exist.
func (m *Type) Read(id string) (*StreamStatus, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.closed {
		return nil, types.ErrTypeClosed
	}

	wrapper, exists := m.streams[id]
	if !exists {
		return nil, ErrStreamDoesNotExist
	}

	return wrapper, nil
}

// Update attempts to stop an existing stream and replace it with a new version
// of the same stream.
func (m *Type) Update(id string, conf stream.Config, timeout time.Duration) error {
	m.lock.Lock()
	wrapper, exists := m.streams[id]
	closed := m.closed
	m.lock.Unlock()

	if closed {
		return types.ErrTypeClosed
	}
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
	if m.closed {
		m.lock.Unlock()
		return types.ErrTypeClosed
	}

	wrapper, exists := m.streams[id]
	m.lock.Unlock()
	if !exists {
		return ErrStreamDoesNotExist
	}

	if err := wrapper.strm.Stop(timeout); err != nil {
		return err
	}

	m.lock.Lock()
	delete(m.streams, id)
	m.lock.Unlock()

	return nil
}

//------------------------------------------------------------------------------

// Stop attempts to gracefully shut down all active streams and close the
// stream manager.
func (m *Type) Stop(timeout time.Duration) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	resultChan := make(chan string)

	for k, v := range m.streams {
		go func(id string, strm *StreamStatus) {
			if err := strm.strm.Stop(timeout); err != nil {
				resultChan <- id
			} else {
				resultChan <- ""
			}
		}(k, v)
	}

	failedStreams := []string{}
	for i := 0; i < len(m.streams); i++ {
		if failedStrm := <-resultChan; len(failedStrm) > 0 {
			failedStreams = append(failedStreams, failedStrm)
		}
	}

	m.streams = map[string]*StreamStatus{}
	m.closed = true

	if len(failedStreams) > 0 {
		return fmt.Errorf("failed to gracefully stop the following streams: %v", failedStreams)
	}
	return nil
}

//------------------------------------------------------------------------------
