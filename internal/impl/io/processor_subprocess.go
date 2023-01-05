package io

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newSubprocess(conf.Subprocess, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("subprocess", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "subprocess",
		Categories: []string{
			"Integration",
		},
		Summary: `
Executes a command as a subprocess and, for each message, will pipe its contents to the stdin stream of the process followed by a newline.`,
		Description: `
The subprocess must then either return a line over stdout or stderr. If a response is returned over stdout then its contents will replace the message. If a response is instead returned from stderr it will be logged and the message will continue unchanged and will be [marked as failed](/docs/configuration/error_handling).

Rather than separating data by a newline it's possible to specify alternative ` + "[`codec_send`](#codec_send) and [`codec_recv`](#codec_recv)" + ` values, which allow binary messages to be encoded for logical separation.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.

The field ` + "`max_buffer`" + ` defines the maximum response size able to be read from the subprocess. This value should be set significantly above the real expected maximum response size.

## Subprocess requirements

It is required that subprocesses flush their stdout and stderr pipes for each line. Benthos will attempt to keep the process alive for as long as the pipeline is running. If the process exits early it will be restarted.

## Messages containing line breaks

If a message contains line breaks each line of the message is piped to the subprocess and flushed, and a response is expected from the subprocess before another line is fed in.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("name", "The command to execute as a subprocess.", "cat", "sed", "awk"),
			docs.FieldString("args", "A list of arguments to provide the command.").Array(),
			docs.FieldInt("max_buffer", "The maximum expected response size.").Advanced(),
			docs.FieldString(
				"codec_send", "Determines how messages written to the subprocess are encoded, which allows them to be logically separated.",
			).HasOptions("lines", "length_prefixed_uint32_be", "netstring").AtVersion("3.37.0").Advanced(),
			docs.FieldString(
				"codec_recv", "Determines how messages read from the subprocess are decoded, which allows them to be logically separated.",
			).HasOptions("lines", "length_prefixed_uint32_be", "netstring").AtVersion("3.37.0").Advanced(),
		).ChildDefaultAndTypesFromStruct(processor.NewSubprocessConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type subprocessProc struct {
	log log.Modular

	subproc  *subprocWrapper
	procFunc func(part *message.Part) error
	mut      sync.Mutex
}

func newSubprocess(conf processor.SubprocessConfig, mgr bundle.NewManagement) (*subprocessProc, error) {
	e := &subprocessProc{
		log: mgr.Logger(),
	}
	var err error
	if e.subproc, err = newSubprocWrapper(conf.Name, conf.Args, conf.MaxBuffer, conf.CodecRecv, mgr.Logger()); err != nil {
		return nil, err
	}
	if e.procFunc, err = e.getSendSubprocessorFunc(conf.CodecSend); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *subprocessProc) getSendSubprocessorFunc(codec string) (func(part *message.Part) error, error) {
	switch codec {
	case "length_prefixed_uint32_be":
		return func(part *message.Part) error {
			const prefixBytes int = 4

			lenBuf := make([]byte, prefixBytes)
			m := part.AsBytes()
			binary.BigEndian.PutUint32(lenBuf, uint32(len(m)))

			res, err := e.subproc.Send(lenBuf, m, nil)
			if err != nil {
				e.log.Errorf("Failed to send message to subprocess: %v\n", err)
				return err
			}
			res2 := make([]byte, len(res))
			copy(res2, res)
			part.SetBytes(res2)
			return nil
		}, nil
	case "netstring":
		return func(part *message.Part) error {
			lenBuf := make([]byte, 0)
			m := part.AsBytes()
			lenBuf = append(strconv.AppendUint(lenBuf, uint64(len(m)), 10), ':')
			res, err := e.subproc.Send(lenBuf, m, commaBytes)
			if err != nil {
				e.log.Errorf("Failed to send message to subprocess: %v\n", err)
				return err
			}
			res2 := make([]byte, len(res))
			copy(res2, res)
			part.SetBytes(res2)
			return nil
		}, nil
	case "lines":
		return func(part *message.Part) error {
			results := [][]byte{}
			splitMsg := bytes.Split(part.AsBytes(), newLineBytes)
			for j, p := range splitMsg {
				if len(p) == 0 && len(splitMsg) > 1 && j == (len(splitMsg)-1) {
					results = append(results, []byte(""))
					continue
				}
				res, err := e.subproc.Send(nil, p, newLineBytes)
				if err != nil {
					e.log.Errorf("Failed to send message to subprocess: %v\n", err)
					return err
				}
				results = append(results, res)
			}
			part.SetBytes(bytes.Join(results, newLineBytes))
			return nil
		}, nil
	}
	return nil, fmt.Errorf("unrecognized codec_send value: %v", codec)
}

type subprocWrapper struct {
	name   string
	args   []string
	maxBuf int

	splitFunc bufio.SplitFunc
	logger    log.Modular

	cmdMut      sync.Mutex
	cmdExitChan chan struct{}
	stdoutChan  chan []byte
	stderrChan  chan []byte

	cmd         *exec.Cmd
	cmdStdin    io.WriteCloser
	cmdCancelFn func()

	shutSig *shutdown.Signaller
}

func newSubprocWrapper(name string, args []string, maxBuf int, codecRecv string, log log.Modular) (*subprocWrapper, error) {
	s := &subprocWrapper{
		name:    name,
		args:    args,
		maxBuf:  maxBuf,
		logger:  log,
		shutSig: shutdown.NewSignaller(),
	}
	switch codecRecv {
	case "lines":
		s.splitFunc = bufio.ScanLines
	case "length_prefixed_uint32_be":
		s.splitFunc = lengthPrefixedUInt32BESplitFunc
	case "netstring":
		s.splitFunc = netstringSplitFunc
	default:
		return nil, fmt.Errorf("invalid codec_recv option: %v", codecRecv)
	}
	if err := s.start(); err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			_ = s.stop()
			s.shutSig.ShutdownComplete()
		}()
		for {
			select {
			case <-s.cmdExitChan:
				log.Warnln("Subprocess exited")
				_ = s.stop()

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

				_ = s.start()
			case <-s.shutSig.CloseAtLeisureChan():
				return
			}
		}
	}()
	return s, nil
}

var maxInt = (1<<bits.UintSize)/2 - 1

func lengthPrefixedUInt32BESplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	const prefixBytes int = 4
	if atEOF {
		return 0, nil, nil
	}
	if len(data) < prefixBytes {
		// request more data
		return 0, nil, nil
	}
	l := binary.BigEndian.Uint32(data)
	if l > (uint32(maxInt) - uint32(prefixBytes)) {
		return 0, nil, errors.New("number of bytes to read exceeds representable range of go int datatype")
	}
	bytesToRead := int(l)

	if len(data)-prefixBytes >= bytesToRead {
		return prefixBytes + bytesToRead, data[prefixBytes : prefixBytes+bytesToRead], nil
	}
	return 0, nil, nil
}

func netstringSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, ':'); i >= 0 {
		if i == 0 {
			return 0, nil, errors.New("encountered invalid netstring: netstring starts with colon (':')")
		}
		l, err := strconv.ParseUint(string(data[0:i]), 10, bits.UintSize-1)
		if err != nil {
			return 0, nil, fmt.Errorf("encountered invalid netstring: unable to decode length '%v'", string(data[0:i]))
		}
		bytesToRead := int(l)

		if len(data) > i+1+bytesToRead {
			if data[i+1+bytesToRead] != ',' {
				return 0, nil, errors.New("encountered invalid netstring: trailing comma-character is missing")
			}
			return i + 1 + bytesToRead + 1, data[i+1 : i+1+bytesToRead], nil
		}
	}
	// request more data
	return 0, nil, nil
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
	if err := cmd.Start(); err != nil {
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
		scanner.Split(s.splitFunc)
		if s.maxBuf != bufio.MaxScanTokenSize {
			scanner.Buffer(nil, s.maxBuf)
		}
		for scanner.Scan() {
			data := scanner.Bytes()
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)

			stdoutChan <- dataCopy
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
			data := scanner.Bytes()
			dataCopy := make([]byte, len(data))
			copy(dataCopy, data)

			stderrChan <- dataCopy
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

func (s *subprocWrapper) Send(prolog, payload, epilog []byte) ([]byte, error) {
	s.cmdMut.Lock()
	stdin := s.cmdStdin
	outChan := s.stdoutChan
	errChan := s.stderrChan
	s.cmdMut.Unlock()

	if stdin == nil {
		return nil, component.ErrTypeClosed
	}
	if prolog != nil {
		if _, err := stdin.Write(prolog); err != nil {
			return nil, err
		}
	}
	if _, err := stdin.Write(payload); err != nil {
		return nil, err
	}
	if epilog != nil {
		if _, err := stdin.Write(epilog); err != nil {
			return nil, err
		}
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
		return nil, component.ErrTypeClosed
	}
	if len(errBytes) > 0 {
		return nil, errors.New(string(errBytes))
	}
	return outBytes, nil
}

//------------------------------------------------------------------------------

var (
	newLineBytes = []byte("\n")
	commaBytes   = []byte(",")
)

// ProcessMessage logs an event and returns the message unchanged.
func (e *subprocessProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	e.mut.Lock()
	defer e.mut.Unlock()

	if err := e.procFunc(msg); err != nil {
		return nil, err
	}
	return []*message.Part{msg}, nil
}

func (e *subprocessProc) Close(ctx context.Context) error {
	e.subproc.shutSig.CloseNow()
	select {
	case <-e.subproc.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
