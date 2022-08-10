package io

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		s, err := newSubprocessWriter(conf.Subprocess, nm.Logger())
		if err != nil {
			return nil, err
		}
		return output.NewAsyncWriter("subprocess", 1, s, nm)
	}), docs.ComponentSpec{
		Name:   "subprocess",
		Status: docs.StatusBeta,
		Summary: `
Executes a command, runs it as a subprocess, and writes messages to it over stdin.`,
		Description: `
Messages are written according to a specified codec. The process is expected to terminate gracefully when stdin is closed.

If the subprocess exits unexpectedly then Benthos will log anything printed to stderr and will log the exit code, and will attempt to execute the command again until success.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("name", "The command to execute as a subprocess."),
			docs.FieldString("args", "A list of arguments to provide the command.").Array(),
			docs.FieldString(
				"codec", "The way in which messages should be written to the subprocess.",
			).HasOptions("lines"),
		).ChildDefaultAndTypesFromStruct(output.NewSubprocessConfig()),
		Categories: []string{
			"Utility",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func subprocOutputLinesCodec(w io.Writer, b []byte) error {
	_, err := fmt.Fprintln(w, string(b))
	return err
}

type subprocOutputCodec func(io.Writer, []byte) error

func subprocOutputCodecFromStr(codec string) (subprocOutputCodec, error) {
	// TODO: Flesh this out with more options based on s.conf.Codec.
	if codec == "lines" {
		return subprocOutputLinesCodec, nil
	}
	return nil, fmt.Errorf("codec not recognised: %v", codec)
}

//------------------------------------------------------------------------------

type subprocessWriter struct {
	log  log.Modular
	conf output.SubprocessConfig

	codec subprocOutputCodec

	cmdMut sync.Mutex
	stdin  io.WriteCloser
}

func newSubprocessWriter(conf output.SubprocessConfig, log log.Modular) (*subprocessWriter, error) {
	s := &subprocessWriter{
		conf: conf,
		log:  log,
	}
	var err error
	if s.codec, err = subprocOutputCodecFromStr(s.conf.Codec); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *subprocessWriter) Connect(ctx context.Context) error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()

	if s.stdin != nil {
		return nil
	}

	cmd := exec.Command(s.conf.Name, s.conf.Args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	go func() {
		stdout, err := cmd.Output()
		if len(stdout) > 0 {
			s.log.Debugf("Process exited with: %s\n", stdout)
		} else {
			s.log.Debugln("Process exited")
		}
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				if len(exitErr.Stderr) > 0 {
					s.log.Errorf("Process exited with error: %s\n", exitErr.Stderr)
				} else if !exitErr.Success() {
					s.log.Errorf("Process exited with code %v: %v\n", exitErr.ExitCode(), exitErr.String())
				}
			} else {
				s.log.Errorf("Process error: %v\n", err)
			}
		}
		s.cmdMut.Lock()
		if s.stdin != nil {
			s.stdin.Close()
			s.stdin = nil
		}
		s.cmdMut.Unlock()
	}()

	s.stdin = stdin
	return nil
}

func (s *subprocessWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()
	if s.stdin == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		return s.codec(s.stdin, p.AsBytes())
	})
}

func (s *subprocessWriter) Close(ctx context.Context) error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()

	var err error
	if s.stdin != nil {
		err = s.stdin.Close()
		s.stdin = nil
	}
	return err
}
