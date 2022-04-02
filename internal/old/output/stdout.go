package output

import (
	"context"
	"os"
	"time"

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSTDOUT] = TypeSpec{
		constructor: fromSimpleConstructor(NewSTDOUT),
		Summary: `
Prints messages to stdout as a continuous stream of data, dividing messages according to the specified codec.`,
		Config: docs.FieldComponent().WithChildren(
			codec.WriterDocs.AtVersion("3.46.0"),
		),
		Categories: []string{
			"Local",
		},
	}
}

//------------------------------------------------------------------------------

// STDOUTConfig contains configuration fields for the stdout based output type.
type STDOUTConfig struct {
	Codec string `json:"codec" yaml:"codec"`
}

// NewSTDOUTConfig creates a new STDOUTConfig with default values.
func NewSTDOUTConfig() STDOUTConfig {
	return STDOUTConfig{
		Codec: "lines",
	}
}

//------------------------------------------------------------------------------

// NewSTDOUT creates a new STDOUT output type.
func NewSTDOUT(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	f, err := newStdoutWriter(conf.STDOUT.Codec, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(TypeSTDOUT, 1, f, log, stats)
	if err != nil {
		return nil, err
	}
	if aw, ok := w.(*AsyncWriter); ok {
		aw.SetNoCancel()
	}
	return w, nil
}

type stdoutWriter struct {
	handle  codec.Writer
	shutSig *shutdown.Signaller
}

func newStdoutWriter(codecStr string, log log.Modular, stats metrics.Type) (*stdoutWriter, error) {
	codec, _, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}

	handle, err := codec(os.Stdout)
	if err != nil {
		return nil, err
	}

	return &stdoutWriter{
		handle:  handle,
		shutSig: shutdown.NewSignaller(),
	}, nil
}

func (w *stdoutWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (w *stdoutWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return writer.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		return w.handle.Write(ctx, p)
	})
}

func (w *stdoutWriter) CloseAsync() {
}

func (w *stdoutWriter) WaitForClose(timeout time.Duration) error {
	return nil
}
