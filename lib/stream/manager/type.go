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
	"net/http"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/stream"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

// streamWrapper tracks a stream along with information regarding its internals.
type streamWrapper struct {
	strm         *stream.Type
	config       stream.Config
	createdAt    time.Time
	stoppedAfter int64
}

func newStreamWrapper(conf stream.Config) *streamWrapper {
	return &streamWrapper{
		config:    conf,
		createdAt: time.Now(),
	}
}

func (s *streamWrapper) IsRunning() bool {
	return atomic.LoadInt64(&s.stoppedAfter) == 0
}

func (s *streamWrapper) Uptime() time.Duration {
	if stoppedAfter := atomic.LoadInt64(&s.stoppedAfter); stoppedAfter > 0 {
		return time.Duration(stoppedAfter)
	}
	return time.Since(s.createdAt)
}

func (s *streamWrapper) Config() stream.Config {
	return s.config
}

func (s *streamWrapper) SetClosed() {
	atomic.SwapInt64(&s.stoppedAfter, int64(time.Since(s.createdAt)))
}

func (s *streamWrapper) SetStream(strm *stream.Type) {
	s.strm = strm
}

//------------------------------------------------------------------------------

type nsMgr struct {
	ns  string
	mgr types.Manager
}

func namespacedMgr(ns string, mgr types.Manager) *nsMgr {
	return &nsMgr{
		ns:  "/" + ns,
		mgr: mgr,
	}
}

// RegisterEndpoint registers a server wide HTTP endpoint.
func (n *nsMgr) RegisterEndpoint(p, desc string, h http.HandlerFunc) {
	n.mgr.RegisterEndpoint(path.Join(n.ns, p), desc, h)
}

// GetCache attempts to find a service wide cache by its name.
func (n *nsMgr) GetCache(name string) (types.Cache, error) {
	return n.mgr.GetCache(name)
}

// GetCondition attempts to find a service wide condition by its name.
func (n *nsMgr) GetCondition(name string) (types.Condition, error) {
	return n.mgr.GetCondition(name)
}

//------------------------------------------------------------------------------

// StreamProcConstructorFunc is a closure type that constructs a processor type
// for new streams, where the id of the stream is provided as an argument.
type StreamProcConstructorFunc func(streamID string) (processor.Type, error)

// StreamPipeConstructorFunc is a closure type that constructs a pipeline type
// for new streams, where the id of the stream is provided as an argument.
type StreamPipeConstructorFunc func(streamID string) (pipeline.Type, error)

//------------------------------------------------------------------------------

// Type manages a collection of streams, providing APIs for CRUD operations on
// the streams.
type Type struct {
	closed  bool
	streams map[string]*streamWrapper

	manager    types.Manager
	stats      metrics.Type
	logger     log.Modular
	apiTimeout time.Duration

	inputPipeCtors    []StreamPipeConstructorFunc
	pipelineProcCtors []StreamProcConstructorFunc
	outputPipeCtors   []StreamPipeConstructorFunc

	lock sync.Mutex
}

// New creates a new stream manager.Type.
func New(opts ...func(*Type)) *Type {
	t := &Type{
		streams:    map[string]*streamWrapper{},
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

// OptAddInputPipelines adds pipeline constructors that will be called for every
// new stream and attached to the input component. The constructor is given the
// name of the stream as an argument.
func OptAddInputPipelines(pipes ...StreamPipeConstructorFunc) func(*Type) {
	return func(t *Type) {
		t.inputPipeCtors = append(t.inputPipeCtors, pipes...)
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

// OptAddOutputPipelines adds pipeline constructors that will be called for
// every new stream and attached to the output component. The constructor is
// given the name of the stream as an argument.
func OptAddOutputPipelines(pipes ...StreamPipeConstructorFunc) func(*Type) {
	return func(t *Type) {
		t.outputPipeCtors = append(t.outputPipeCtors, pipes...)
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

	var inputPipeCtors []pipeline.ConstructorFunc
	var procCtors []pipeline.ProcConstructorFunc
	var outputPipeCtors []pipeline.ConstructorFunc

	for _, ctor := range m.inputPipeCtors {
		func(c StreamPipeConstructorFunc) {
			inputPipeCtors = append(inputPipeCtors, func() (pipeline.Type, error) {
				return c(id)
			})
		}(ctor)
	}
	for _, ctor := range m.pipelineProcCtors {
		func(c StreamProcConstructorFunc) {
			procCtors = append(procCtors, func() (processor.Type, error) {
				return c(id)
			})
		}(ctor)
	}
	for _, ctor := range m.outputPipeCtors {
		func(c StreamPipeConstructorFunc) {
			outputPipeCtors = append(outputPipeCtors, func() (pipeline.Type, error) {
				return c(id)
			})
		}(ctor)
	}

	wrapper := newStreamWrapper(conf)
	strm, err := stream.New(
		conf,
		stream.OptAddInputPipelines(inputPipeCtors...),
		stream.OptAddProcessors(procCtors...),
		stream.OptAddOutputPipelines(outputPipeCtors...),
		stream.OptSetLogger(m.logger.NewModule("."+id)),
		stream.OptSetStats(metrics.Namespaced(m.stats, id)),
		stream.OptSetManager(namespacedMgr(id, m.manager)),
		stream.OptOnClose(func() {
			wrapper.SetClosed()
		}),
	)
	if err != nil {
		return err
	}

	wrapper.SetStream(strm)

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
	if m.closed {
		return status, types.ErrTypeClosed
	}

	wrapper, exists := m.streams[id]
	if !exists {
		return status, ErrStreamDoesNotExist
	}

	status.Active = wrapper.IsRunning()
	status.Config = wrapper.Config()
	status.Uptime = wrapper.Uptime()

	return status, nil
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
		go func(id string, strm *streamWrapper) {
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

	m.streams = map[string]*streamWrapper{}
	m.closed = true

	if len(failedStreams) > 0 {
		return fmt.Errorf("failed to gracefully stop the following streams: %v", failedStreams)
	}
	return nil
}

//------------------------------------------------------------------------------
