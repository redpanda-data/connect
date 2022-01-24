package output

import (
	"context"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var multipartCodecDoc = (`
## Batches and Mulipart Messages

When writing multipart (batched) messages using the ` + "`lines`" + ` codec the last message ends with double delimiters. E.g. the messages "foo", "bar" and "baz" would be written as:

` + "```" + `
foo\n
bar\n
baz\n
` + "```" + `

Whereas a multipart message [ "foo", "bar", "baz" ] would be written as:

` + "```" + `
foo\n
bar\n
baz\n\n
` + "```" + `

This enables consumers of this output feed to reconstruct the original batches. However, if you wish to avoid this behaviour then add a ` + "[`split` processor](/docs/components/processors/split)" + ` before messages reach this output.`)[1:]

func init() {
	Constructors[TypeSTDOUT] = TypeSpec{
		constructor: fromSimpleConstructor(NewSTDOUT),
		Summary: `
Prints messages to stdout as a continuous stream of data, dividing messages according to the specified codec.`,
		Description: multipartCodecDoc,
		FieldSpecs: docs.FieldSpecs{
			codec.WriterDocs.AtVersion("3.46.0"),
		},
		Categories: []Category{
			CategoryLocal,
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
func NewSTDOUT(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
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

func (w *stdoutWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	err := writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		return w.handle.Write(ctx, p)
	})
	if err != nil {
		return err
	}
	if msg.Len() > 1 {
		if w.handle != nil {
			w.handle.EndBatch()
		}
	}
	return nil
}

func (w *stdoutWriter) CloseAsync() {
}

func (w *stdoutWriter) WaitForClose(timeout time.Duration) error {
	return nil
}
