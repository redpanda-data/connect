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

package processor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSubprocess] = TypeSpec{
		constructor: NewSubprocess,
		description: `
Subprocess is a processor that runs a process in the background and, for each
message, will pipe its contents to the stdin stream of the process followed by a
newline.

The subprocess must then either return a line over stdout or stderr. If a
response is returned over stdout then its contents will replace the message. If
a response is instead returned from stderr it will be logged and the message
will continue unchanged and will be [marked as failed](../error_handling.md).

The field ` + "`max_buffer`" + ` defines the maximum response size able to be
read from the subprocess. This value should be set significantly above the real
expected maximum response size.

#### Subprocess requirements

It is required that subprocesses flush their stdout and stderr pipes for each
line.

Benthos will attempt to keep the process alive for as long as the pipeline is
running. If the process exits early it will be restarted.

#### Messages containing line breaks

If a message contains line breaks each line of the message is piped to the
subprocess and flushed, and a response is expected from the subprocess before
another line is fed in.`,
	}
}

//------------------------------------------------------------------------------

// SubprocessConfig contains configuration fields for the Subprocess processor.
type SubprocessConfig struct {
	Parts     []int    `json:"parts" yaml:"parts"`
	Name      string   `json:"name" yaml:"name"`
	Args      []string `json:"args" yaml:"args"`
	MaxBuffer int      `json:"max_buffer" yaml:"max_buffer"`
}

// NewSubprocessConfig returns a SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Parts:     []int{},
		Name:      "cat",
		Args:      []string{},
		MaxBuffer: bufio.MaxScanTokenSize,
	}
}

//------------------------------------------------------------------------------

// Subprocess is a processor that executes a command.
type Subprocess struct {
	subprocClosed int32

	log   log.Modular
	stats metrics.Type

	conf    SubprocessConfig
	subproc *subprocWrapper

	mut sync.Mutex

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSubprocess returns a Subprocess processor.
func NewSubprocess(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	e := &Subprocess{
		log:        log,
		stats:      stats,
		conf:       conf.Subprocess,
		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	var err error
	if e.subproc, err = newSubprocWrapper(conf.Subprocess.Name, conf.Subprocess.Args, e.conf.MaxBuffer, log); err != nil {
		return nil, err
	}
	return e, nil
}

//------------------------------------------------------------------------------

type subprocWrapper struct {
	name   string
	args   []string
	maxBuf int

	logger log.Modular

	cmdMut      sync.Mutex
	cmdExitChan chan struct{}
	stdoutChan  chan []byte
	stderrChan  chan []byte

	cmd         *exec.Cmd
	cmdStdin    io.WriteCloser
	cmdCancelFn func()

	closeChan  chan struct{}
	closedChan chan struct{}
}

func newSubprocWrapper(name string, args []string, maxBuf int, log log.Modular) (*subprocWrapper, error) {
	s := &subprocWrapper{
		name:       name,
		args:       args,
		maxBuf:     maxBuf,
		logger:     log,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}
	if err := s.start(); err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			s.stop()
			close(s.closedChan)
		}()
		for {
			select {
			case <-s.cmdExitChan:
				log.Warnln("Subprocess exited")
				s.stop()

				// Flush channels
				var msgBytes []byte
				for stdoutMsg := range s.stdoutChan {
					msgBytes = append(msgBytes, stdoutMsg...)
				}
				if len(msgBytes) > 0 {
					log.Infoln(string(msgBytes))
				}
				msgBytes = nil
				for stderrMsg := range s.stderrChan {
					msgBytes = append(msgBytes, stderrMsg...)
				}
				if len(msgBytes) > 0 {
					log.Errorln(string(msgBytes))
				}

				s.start()
			case <-s.closeChan:
				return
			}
		}
	}()
	return s, nil
}

func (s *subprocWrapper) start() error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()

	var err error
	cmdCtx, cmdCancelFn := context.WithCancel(context.Background())
	defer func() {
		if err != nil {
			cmdCancelFn()
		}
	}()

	cmd := exec.CommandContext(cmdCtx, s.name, s.args...)
	var cmdStdin io.WriteCloser
	if cmdStdin, err = cmd.StdinPipe(); err != nil {
		return err
	}
	var cmdStdout, cmdStderr io.ReadCloser
	if cmdStdout, err = cmd.StdoutPipe(); err != nil {
		return err
	}
	if cmdStderr, err = cmd.StderrPipe(); err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}

	s.cmd = cmd
	s.cmdStdin = cmdStdin
	s.cmdCancelFn = cmdCancelFn

	cmdExitChan := make(chan struct{})
	stdoutChan := make(chan []byte)
	stderrChan := make(chan []byte)

	go func() {
		defer func() {
			s.cmdMut.Lock()
			if cmdExitChan != nil {
				close(cmdExitChan)
				cmdExitChan = nil
			}
			close(stdoutChan)
			s.cmdMut.Unlock()
		}()

		scanner := bufio.NewScanner(cmdStdout)
		if s.maxBuf != bufio.MaxScanTokenSize {
			scanner.Buffer(nil, s.maxBuf)
		}
		for scanner.Scan() {
			stdoutChan <- scanner.Bytes()
		}
		if err := scanner.Err(); err != nil {
			s.logger.Errorf("Failed to read subprocess output: %v\n", err)
		}
	}()
	go func() {
		defer func() {
			s.cmdMut.Lock()
			if cmdExitChan != nil {
				close(cmdExitChan)
				cmdExitChan = nil
			}
			close(stderrChan)
			s.cmdMut.Unlock()
		}()

		scanner := bufio.NewScanner(cmdStderr)
		if s.maxBuf != bufio.MaxScanTokenSize {
			scanner.Buffer(nil, s.maxBuf)
		}
		for scanner.Scan() {
			stderrChan <- scanner.Bytes()
		}
		if err := scanner.Err(); err != nil {
			s.logger.Errorf("Failed to read subprocess error output: %v\n", err)
		}
	}()

	s.cmdExitChan = cmdExitChan
	s.stdoutChan = stdoutChan
	s.stderrChan = stderrChan
	s.logger.Infoln("Subprocess started")
	return nil
}

func (s *subprocWrapper) stop() error {
	s.cmdMut.Lock()
	var err error
	if s.cmd != nil {
		s.cmdCancelFn()
		err = s.cmd.Wait()
		s.cmd = nil
		s.cmdStdin = nil
		s.cmdCancelFn = func() {}
	}
	s.cmdMut.Unlock()
	return err
}

func (s *subprocWrapper) Send(line []byte) ([]byte, error) {
	s.cmdMut.Lock()
	stdin := s.cmdStdin
	outChan := s.stdoutChan
	errChan := s.stderrChan
	s.cmdMut.Unlock()

	if stdin == nil {
		return nil, types.ErrTypeClosed
	}
	if _, err := stdin.Write(line); err != nil {
		return nil, err
	}
	if _, err := stdin.Write([]byte("\n")); err != nil {
		return nil, err
	}

	var outBytes, errBytes []byte
	var open bool
	select {
	case outBytes, open = <-outChan:
	case errBytes, open = <-errChan:
		tout := time.After(time.Second)
		var errBuf bytes.Buffer
		errBuf.Write(errBytes)
	flushErrLoop:
		for open {
			select {
			case errBytes, open = <-errChan:
				errBuf.Write(errBytes)
			case <-tout:
				break flushErrLoop
			}
		}
		errBytes = errBuf.Bytes()
	}

	if !open {
		return nil, types.ErrTypeClosed
	}
	if len(errBytes) > 0 {
		return nil, errors.New(string(errBytes))
	}
	return outBytes, nil
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (e *Subprocess) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	e.mCount.Incr(1)
	e.mut.Lock()
	defer e.mut.Unlock()

	result := msg.Copy()

	proc := func(i int) error {
		span := tracing.CreateChildSpan(TypeSubprocess, result.Get(i))
		defer span.Finish()

		results := [][]byte{}
		splitMsg := bytes.Split(result.Get(i).Get(), []byte("\n"))
		for j, p := range splitMsg {
			if len(p) == 0 && len(splitMsg) > 1 && j == (len(splitMsg)-1) {
				results = append(results, []byte(""))
				continue
			}
			res, err := e.subproc.Send(p)
			if err != nil {
				e.log.Errorf("Failed to send message to subprocess: %v\n", err)
				e.mErr.Incr(1)
				span.LogFields(
					olog.String("event", "error"),
					olog.String("type", err.Error()),
				)
				results = append(results, p)
			} else {
				results = append(results, res)
			}
		}
		result.Get(i).Set(bytes.Join(results, []byte("\n")))
		return nil
	}

	if len(e.conf.Parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			if err := proc(i); err != nil {
				e.mErr.Incr(1)
				return nil, response.NewError(err)
			}
		}
	} else {
		for _, i := range e.conf.Parts {
			if err := proc(i); err != nil {
				e.mErr.Incr(1)
				return nil, response.NewError(err)
			}
		}
	}

	e.mSent.Incr(int64(result.Len()))
	e.mBatchSent.Incr(1)

	msgs := [1]types.Message{result}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (e *Subprocess) CloseAsync() {
	if atomic.CompareAndSwapInt32(&e.subprocClosed, 0, 1) {
		close(e.subproc.closeChan)
	}
}

// WaitForClose blocks until the processor has closed down.
func (e *Subprocess) WaitForClose(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return types.ErrTimeout
	case <-e.subproc.closedChan:
	}
	return nil
}

//------------------------------------------------------------------------------
