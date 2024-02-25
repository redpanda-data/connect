package io

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	soFieldName  = "name"
	soFieldArgs  = "args"
	soFieldCodec = "codec"
)

func subprocOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Utility").
		Summary("Executes a command, runs it as a subprocess, and writes messages to it over stdin.").
		Description(`
Messages are written according to a specified codec. The process is expected to terminate gracefully when stdin is closed.

If the subprocess exits unexpectedly then Benthos will log anything printed to stderr and will log the exit code, and will attempt to execute the command again until success.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`).
		Fields(
			service.NewStringField(soFieldName).
				Description("The command to execute as a subprocess."),
			service.NewStringListField(soFieldArgs).
				Description("A list of arguments to provide the command.").
				Default([]any{}),
			service.NewStringEnumField(soFieldCodec, "lines").
				Description("The way in which messages should be written to the subprocess.").
				Default("lines"),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"subprocess", subprocOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1
			out, err = newSubprocessWriterFromParsed(conf, mgr.Logger())
			return
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
	log  *service.Logger
	name string
	args []string

	codec subprocOutputCodec

	cmdMut sync.Mutex
	stdin  io.WriteCloser
}

func newSubprocessWriterFromParsed(conf *service.ParsedConfig, log *service.Logger) (s *subprocessWriter, err error) {
	s = &subprocessWriter{log: log}
	if s.name, err = conf.FieldString(soFieldName); err != nil {
		return
	}
	if s.args, err = conf.FieldStringList(soFieldArgs); err != nil {
		return
	}

	var codecStr string
	if codecStr, err = conf.FieldString(soFieldCodec); err != nil {
		return
	}
	if s.codec, err = subprocOutputCodecFromStr(codecStr); err != nil {
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

	cmd := exec.Command(s.name, s.args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	go func() {
		stdout, err := cmd.Output()
		if len(stdout) > 0 {
			s.log.Debugf("Process exited with: %s\n", stdout)
		} else {
			s.log.Debug("Process exited")
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

func (s *subprocessWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()
	if s.stdin == nil {
		return component.ErrNotConnected
	}

	return b.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		mBytes, err := m.AsBytes()
		if err != nil {
			return err
		}
		return s.codec(s.stdin, mBytes)
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
