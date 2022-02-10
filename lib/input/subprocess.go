package input

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSubprocess] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
			b, err := newSubprocess(conf.Subprocess)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(TypeSubprocess, true, b, log, stats)
		}),
		Status: docs.StatusBeta,
		Summary: `
Executes a command, runs it as a subprocess, and consumes messages from it over stdout.`,
		Description: `
Messages are consumed according to a specified codec. The command is executed once and if it terminates the input also closes down gracefully. Alternatively, the field ` + "`restart_on_close` can be set to `true`" + ` in order to have Benthos re-execute the command each time it stops.

The field ` + "`max_buffer`" + ` defines the maximum message size able to be read from the subprocess. This value should be set significantly above the real expected maximum message size.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("name", "The command to execute as a subprocess.", "cat", "sed", "awk"),
			docs.FieldString("args", "A list of arguments to provide the command.").Array(),
			docs.FieldCommon(
				"codec", "The way in which messages should be consumed from the subprocess.",
			).HasOptions("lines"),
			docs.FieldCommon("restart_on_exit", "Whether the command should be re-executed each time the subprocess ends."),
			docs.FieldAdvanced("max_buffer", "The maximum expected size of an individual message."),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

type subprocScanner interface {
	Bytes() []byte
	Text() string
	Err() error
	Scan() bool
}

func linesSubprocCodec(conf SubprocessConfig, stdout, stderr io.Reader) (outScanner, errScanner subprocScanner) {
	outScanner = bufio.NewScanner(stdout)
	errScanner = bufio.NewScanner(stderr)
	if conf.MaxBuffer != bufio.MaxScanTokenSize {
		outScanner.(*bufio.Scanner).Buffer([]byte{}, conf.MaxBuffer)
		errScanner.(*bufio.Scanner).Buffer([]byte{}, conf.MaxBuffer)
	}
	return outScanner, errScanner
}

type subprocCodec func(SubprocessConfig, io.Reader, io.Reader) (subprocScanner, subprocScanner)

func codecFromStr(codec string) (subprocCodec, error) {
	// TODO: Flesh this out with more options based on s.conf.Codec.
	if codec == "lines" {
		return linesSubprocCodec, nil
	}
	return nil, fmt.Errorf("codec not recognised: %v", codec)
}

//------------------------------------------------------------------------------

// SubprocessConfig contains configuration for the Subprocess input type.
type SubprocessConfig struct {
	Name          string   `json:"name" yaml:"name"`
	Args          []string `json:"args" yaml:"args"`
	Codec         string   `json:"codec" yaml:"codec"`
	RestartOnExit bool     `json:"restart_on_exit" yaml:"restart_on_exit"`
	MaxBuffer     int      `json:"max_buffer" yaml:"max_buffer"`
}

// NewSubprocessConfig creates a new SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Name:          "",
		Args:          []string{},
		Codec:         "lines",
		RestartOnExit: false,
		MaxBuffer:     bufio.MaxScanTokenSize,
	}
}

// Subprocess executes a bloblang mapping with an empty context each time this
// input is read from. An interval period must be specified that determines how
// often a message is generated.
type Subprocess struct {
	conf  SubprocessConfig
	codec subprocCodec

	msgChan chan []byte
	errChan chan error

	close func()
	ctx   context.Context
}

// newSubprocess creates a new bloblang input reader type.
func newSubprocess(conf SubprocessConfig) (*Subprocess, error) {
	s := &Subprocess{
		conf: conf,
	}
	s.ctx, s.close = context.WithCancel(context.Background())
	var err error
	if s.codec, err = codecFromStr(s.conf.Codec); err != nil {
		return nil, err
	}
	return s, nil
}

// ConnectWithContext establishes a Subprocess reader.
func (s *Subprocess) ConnectWithContext(ctx context.Context) error {
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

// ReadWithContext a new bloblang generated message.
func (s *Subprocess) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
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
		msg := message.QuickBatch(nil)
		msg.Append(message.NewPart(b))
		return msg, func(context.Context, response.Error) error { return nil }, nil
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

// CloseAsync shuts down the bloblang reader.
func (s *Subprocess) CloseAsync() {
	s.close()
}

// WaitForClose blocks until the bloblang input has closed down.
func (s *Subprocess) WaitForClose(timeout time.Duration) error {
	return nil
}
