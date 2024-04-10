package io

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	spiFieldName          = "name"
	spiFieldArgs          = "args"
	spiFieldCodec         = "codec"
	spiFieldRestartOnExit = "restart_on_exit"
	spiFieldMaxBuffer     = "max_buffer"
)

func subprocInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Utility").
		Summary("Executes a command, runs it as a subprocess, and consumes messages from it over stdout.").
		Description(`
Messages are consumed according to a specified codec. The command is executed once and if it terminates the input also closes down gracefully. Alternatively, the field `+"`restart_on_close` can be set to `true`"+` in order to have Benthos re-execute the command each time it stops.

The field `+"`max_buffer`"+` defines the maximum message size able to be read from the subprocess. This value should be set significantly above the real expected maximum message size.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`).
		Fields(
			service.NewStringField(spiFieldName).
				Description("The command to execute as a subprocess.").
				Examples("cat", "sed", "awk"),
			service.NewStringListField(spiFieldArgs).
				Description("A list of arguments to provide the command.").
				Default([]any{}),
			service.NewStringEnumField(spiFieldCodec, "lines").
				Description("The way in which messages should be consumed from the subprocess.").
				Default("lines"),
			service.NewBoolField(spiFieldRestartOnExit).
				Description("Whether the command should be re-executed each time the subprocess ends.").
				Default(false),
			service.NewIntField(spiFieldMaxBuffer).
				Description("The maximum expected size of an individual message.").
				Advanced().
				Default(bufio.MaxScanTokenSize),
		)
}

func init() {
	err := service.RegisterBatchInput("subprocess", subprocInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		return newSubprocessReaderFromParsed(conf)
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

func linesSubprocInputCodec(maxBuf int, stdout, stderr io.Reader) (outScanner, errScanner inputSubprocScanner) {
	outScanner = bufio.NewScanner(stdout)
	errScanner = bufio.NewScanner(stderr)
	if maxBuf != bufio.MaxScanTokenSize {
		outScanner.(*bufio.Scanner).Buffer([]byte{}, maxBuf)
		errScanner.(*bufio.Scanner).Buffer([]byte{}, maxBuf)
	}
	return outScanner, errScanner
}

type subprocInputCodec func(int, io.Reader, io.Reader) (inputSubprocScanner, inputSubprocScanner)

func subprocInputCodecFromStr(codec string) (subprocInputCodec, error) {
	// TODO: Flesh this out with more options based on s.conf.Codec.
	if codec == "lines" {
		return linesSubprocInputCodec, nil
	}
	return nil, fmt.Errorf("codec not recognised: %v", codec)
}

//------------------------------------------------------------------------------

type subprocessReader struct {
	name          string
	args          []string
	restartOnExit bool
	maxBuf        int
	codec         subprocInputCodec

	msgChan chan []byte
	errChan chan error

	close func()
	ctx   context.Context
}

func newSubprocessReaderFromParsed(conf *service.ParsedConfig) (s *subprocessReader, err error) {
	s = &subprocessReader{}
	s.ctx, s.close = context.WithCancel(context.Background())

	if s.name, err = conf.FieldString(spiFieldName); err != nil {
		return
	}
	if s.args, err = conf.FieldStringList(spiFieldArgs); err != nil {
		return
	}
	if s.restartOnExit, err = conf.FieldBool(spiFieldRestartOnExit); err != nil {
		return
	}
	if s.maxBuf, err = conf.FieldInt(spiFieldMaxBuffer); err != nil {
		return
	}

	var codecStr string
	if codecStr, err = conf.FieldString(spiFieldCodec); err != nil {
		return nil, err
	}

	if s.codec, err = subprocInputCodecFromStr(codecStr); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *subprocessReader) Connect(ctx context.Context) error {
	if s.msgChan != nil {
		return nil
	}

	cmd := exec.CommandContext(s.ctx, s.name, s.args...)

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

	outScanner, errScanner := s.codec(s.maxBuf, stdout, stderr)

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

func (s *subprocessReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	msgChan, errChan := s.msgChan, s.errChan
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case b, open := <-msgChan:
		if !open {
			if s.restartOnExit {
				s.msgChan = nil
				s.errChan = nil
				return nil, nil, service.ErrNotConnected
			}
			return nil, nil, service.ErrEndOfInput
		}
		msg := service.MessageBatch{service.NewMessage(b)}
		return msg, func(context.Context, error) error { return nil }, nil
	case err, open := <-errChan:
		if !open {
			if s.restartOnExit {
				s.msgChan = nil
				s.errChan = nil
				return nil, nil, service.ErrNotConnected
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
