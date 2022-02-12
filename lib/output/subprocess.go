package output

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
)

func init() {
	Constructors[TypeSubprocess] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
			s, err := newSubprocess(conf.Subprocess, log)
			if err != nil {
				return nil, err
			}
			return NewAsyncWriter(TypeSubprocess, 1, s, log, stats)
		}),
		Status: docs.StatusBeta,
		Summary: `
Executes a command, runs it as a subprocess, and writes messages to it over stdin.`,
		Description: `
Messages are written according to a specified codec. The process is expected to terminate gracefully when stdin is closed.

If the subprocess exits unexpectedly then Benthos will log anything printed to stderr and will log the exit code, and will attempt to execute the command again until success.

The execution environment of the subprocess is the same as the Benthos instance, including environment variables and the current working directory.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("name", "The command to execute as a subprocess."),
			docs.FieldString("args", "A list of arguments to provide the command.").Array(),
			docs.FieldCommon(
				"codec", "The way in which messages should be written to the subprocess.",
			).HasOptions("lines"),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

func linesCodec(w io.Writer, b []byte) error {
	_, err := fmt.Fprintln(w, string(b))
	return err
}

type subprocCodec func(io.Writer, []byte) error

func codecFromStr(codec string) (subprocCodec, error) {
	// TODO: Flesh this out with more options based on s.conf.Codec.
	if codec == "lines" {
		return linesCodec, nil
	}
	return nil, fmt.Errorf("codec not recognised: %v", codec)
}

//------------------------------------------------------------------------------

// SubprocessConfig contains configuration for the Subprocess input type.
type SubprocessConfig struct {
	Name  string   `json:"name" yaml:"name"`
	Args  []string `json:"args" yaml:"args"`
	Codec string   `json:"codec" yaml:"codec"`
}

// NewSubprocessConfig creates a new SubprocessConfig with default values.
func NewSubprocessConfig() SubprocessConfig {
	return SubprocessConfig{
		Name:  "",
		Args:  []string{},
		Codec: "lines",
	}
}

//------------------------------------------------------------------------------

// Subprocess executes a bloblang mapping with an empty context each time this
// input is read from. An interval period must be specified that determines how
// often a message is generated.
type Subprocess struct {
	log  log.Modular
	conf SubprocessConfig

	codec subprocCodec

	cmdMut sync.Mutex
	stdin  io.WriteCloser
}

// newSubprocess creates a new bloblang input reader type.
func newSubprocess(conf SubprocessConfig, log log.Modular) (*Subprocess, error) {
	s := &Subprocess{
		conf: conf,
		log:  log,
	}
	var err error
	if s.codec, err = codecFromStr(s.conf.Codec); err != nil {
		return nil, err
	}
	return s, nil
}

// ConnectWithContext establishes a Subprocess reader.
func (s *Subprocess) ConnectWithContext(ctx context.Context) error {
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

// WriteWithContext attempts to write message contents to a directory as files.
func (s *Subprocess) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	s.cmdMut.Lock()
	defer s.cmdMut.Unlock()
	if s.stdin == nil {
		return component.ErrNotConnected
	}

	return writer.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		return s.codec(s.stdin, p.Get())
	})
}

// CloseAsync shuts down the bloblang reader.
func (s *Subprocess) CloseAsync() {
	s.cmdMut.Lock()
	if s.stdin != nil {
		s.stdin.Close()
		s.stdin = nil
	}
	s.cmdMut.Unlock()
}

// WaitForClose blocks until the bloblang input has closed down.
func (s *Subprocess) WaitForClose(timeout time.Duration) error {
	return nil
}
