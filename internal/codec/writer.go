package codec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// WriterDocs is a static field documentation for output codecs.
var WriterDocs = docs.FieldString(
	"codec", "The way in which the bytes of messages should be written out into the output data stream. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.", "lines", "delim:\t", "delim:foobar",
).HasAnnotatedOptions(
	"all-bytes", "Only applicable to file based outputs. Writes each message to a file in full, if the file already exists the old content is deleted.",
	"append", "Append each message to the output stream without any delimiter or special encoding.",
	"lines", "Append each message to the output stream followed by a line break.",
	"delim:x", "Append each message to the output stream followed by a custom delimiter.",
)

//------------------------------------------------------------------------------

// Writer is a codec type that reads message parts from a source.
type Writer interface {
	Write(context.Context, *message.Part) error
	Close(context.Context) error
}

// WriterConfig contains custom configuration specific to a codec describing how
// handles should be provided.
type WriterConfig struct {
	Append     bool
	Truncate   bool
	CloseAfter bool
}

// WriterConstructor creates a writer from an io.WriteCloser.
type WriterConstructor func(io.WriteCloser) (Writer, error)

// GetWriter returns a constructor that creates write codecs.
func GetWriter(codec string) (WriterConstructor, WriterConfig, error) {
	switch codec {
	case "all-bytes":
		return func(w io.WriteCloser) (Writer, error) {
			return &allBytesWriter{w}, nil
		}, allBytesConfig, nil
	case "append":
		return func(w io.WriteCloser) (Writer, error) {
			return newCustomDelimWriter(w, "")
		}, customDelimConfig, nil
	case "lines":
		return newLinesWriter, linesWriterConfig, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if by == "" {
			return nil, WriterConfig{}, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return func(w io.WriteCloser) (Writer, error) {
			return newCustomDelimWriter(w, by)
		}, customDelimConfig, nil
	}
	return nil, WriterConfig{}, fmt.Errorf("codec was not recognised: %v", codec)
}

//------------------------------------------------------------------------------

var allBytesConfig = WriterConfig{
	Truncate:   true,
	CloseAfter: true,
}

type allBytesWriter struct {
	o io.WriteCloser
}

func (a *allBytesWriter) Write(ctx context.Context, msg *message.Part) error {
	_, err := a.o.Write(msg.AsBytes())
	return err
}

func (a *allBytesWriter) Close(ctx context.Context) error {
	return a.o.Close()
}

//------------------------------------------------------------------------------

var linesWriterConfig = WriterConfig{
	Append: true,
}

type linesWriter struct {
	w io.WriteCloser
}

func newLinesWriter(w io.WriteCloser) (Writer, error) {
	return &linesWriter{w: w}, nil
}

func (l *linesWriter) Write(ctx context.Context, p *message.Part) error {
	partBytes := p.AsBytes()
	if _, err := l.w.Write(partBytes); err != nil {
		return err
	}
	if !bytes.HasSuffix(partBytes, []byte("\n")) {
		_, err := l.w.Write([]byte("\n"))
		return err
	}
	return nil
}

func (l *linesWriter) Close(ctx context.Context) error {
	return l.w.Close()
}

//------------------------------------------------------------------------------

var customDelimConfig = WriterConfig{
	Append: true,
}

type customDelimWriter struct {
	w     io.WriteCloser
	delim []byte
}

func newCustomDelimWriter(w io.WriteCloser, delim string) (Writer, error) {
	delimBytes := []byte(delim)
	return &customDelimWriter{w: w, delim: delimBytes}, nil
}

func (d *customDelimWriter) Write(ctx context.Context, p *message.Part) error {
	partBytes := p.AsBytes()
	if _, err := d.w.Write(partBytes); err != nil {
		return err
	}
	if !bytes.HasSuffix(partBytes, d.delim) {
		_, err := d.w.Write(d.delim)
		return err
	}
	return nil
}

func (d *customDelimWriter) Close(ctx context.Context) error {
	return d.w.Close()
}
