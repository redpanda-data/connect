package io

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		b, err := newSubprocessReader(conf.Subprocess)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("subprocess", b, nm)
	}), docs.ComponentSpec{
		Name:   "subprocess",
		Status: docs.StatusBeta,
		Summary: `
Executes a command, runs it as a subprocess, and consumes messages from it over stdout.`,
		Description: `
Messages are consumed according to a specified codec. The command is executed once and if it terminates the input also closes down gracefully. Alternatively, the field ` + "`restart_on_close` can be set to `true`" + ` in order to have Benthos re-execute the command each time it stops.

The field ` + "`max_buffer`" + ` defines the maximum message size able to be read from the subprocess. This value should be set significantly above the real expected maximum message size.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("name", "The command to execute as a subprocess.", "cat", "sed", "awk"),
			docs.FieldString("args", "A list of arguments to provide the command.").Array(),
			docs.FieldString(
				"codec", "The way in which messages should be consumed from the subprocess.",
			).HasOptions("lines"),
			docs.FieldBool("restart_on_exit", "Whether the command should be re-executed each time the subprocess ends."),
			docs.FieldInt("max_buffer", "The maximum expected size of an individual message.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewSubprocessConfig()),
		Categories: []string{
			"Utility",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type inputSubprocScanner interface {
	Bytes() []byte
	Text() string
	Err() error
	Scan() bool
}

func linesSubprocInputCodec(conf input.SubprocessConfig, stdout, stderr io.Reader) (outScanner, errScanner inputSubprocScanner) {
	outScanner = bufio.NewScanner(stdout)
	errScanner = bufio.NewScanner(stderr)
	if conf.MaxBuffer != bufio.MaxScanTokenSize {
		outScanner.(*bufio.Scanner).Buffer([]byte{}, conf.MaxBuffer)
		errScanner.(*bufio.Scanner).Buffer([]byte{}, conf.MaxBuffer)
	}
	return outScanner, errScanner
}

type subprocInputCodec func(input.SubprocessConfig, io.Reader, io.Reader) (inputSubprocScanner, inputSubprocScanner)

func subprocInputCodecFromStr(codec string) (subprocInputCodec, error) {
	// TODO: Flesh this out with more options based on s.conf.Codec.
	if codec == "lines" {
		return linesSubprocInputCodec, nil
	}
	return nil, fmt.Errorf("codec not recognised: %v", codec)
}

//------------------------------------------------------------------------------

type subprocessReader struct {
	conf  input.SubprocessConfig
	codec subprocInputCodec

	msgChan chan []byte
	errChan chan error

	close func()
	ctx   context.Context
}

func newSubprocessReader(conf input.SubprocessConfig) (*subprocessReader, error) {
	s := &subprocessReader{
		conf: conf,
	}
	s.ctx, s.close = context.WithCancel(context.Background())
	var err error
	if s.codec, err = subprocInputCodecFromStr(s.conf.Codec); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *subprocessReader) Connect(ctx context.Context) error {
	if s.msgChan != nil {
		return nil
	}

	cmd := exec.CommandContext(s.ctx, s.conf.Name, s.conf.Args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	msgChan := make(chan []byte)
	errChan := make(chan error)

	outScanner, errScanner := s.codec(s.conf, stdout, stderr)

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()

			for outScanner.Scan() {
				data := outScanner.Bytes()
				dataCopy := make([]byte, len(data))
				copy(dataCopy, data)

				select {
				case msgChan <- dataCopy:
				case <-s.ctx.Done():
				}
			}

			if err := outScanner.Err(); err != nil {
				select {
				case errChan <- err:
				case <-s.ctx.Done():
				}
			}
		}()

		go func() {
			defer wg.Done()

			for errScanner.Scan() {
				select {
				case errChan <- errors.New(errScanner.Text()):
				case <-s.ctx.Done():
				}
			}

			if err := errScanner.Err(); err != nil {
				select {
				case errChan <- err:
				case <-s.ctx.Done():
				}
			}
		}()

		wg.Wait()
		close(msgChan)
		close(errChan)
	}()

	s.msgChan = msgChan
	s.errChan = errChan
	return nil
}

func (s *subprocessReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	msgChan, errChan := s.msgChan, s.errChan
	if msgChan == nil {
		return nil, nil, component.ErrNotConnected
	}

	select {
	case b, open := <-msgChan:
		if !open {
			if s.conf.RestartOnExit {
				s.msgChan = nil
				s.errChan = nil
				return nil, nil, component.ErrNotConnected
			}
			return nil, nil, component.ErrTypeClosed
		}
		msg := message.Batch{message.NewPart(b)}
		return msg, func(context.Context, error) error { return nil }, nil
	case err, open := <-errChan:
		if !open {
			if s.conf.RestartOnExit {
				s.msgChan = nil
				s.errChan = nil
				return nil, nil, component.ErrNotConnected
			}
			return nil, nil, component.ErrTypeClosed
		}
		return nil, nil, err
	case <-ctx.Done():
	}

	return nil, nil, component.ErrTimeout
}

func (s *subprocessReader) Close(ctx context.Context) (err error) {
	s.close()
	return
}
