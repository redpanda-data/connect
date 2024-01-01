package pure

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	slFieldCustomDelimiter = "custom_delimiter"
	slFieldMaxBufferSize   = "max_buffer_size"
)

func linesScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Split an input stream into a message per line of data.").
		Fields(
			service.NewStringField(slFieldCustomDelimiter).
				Description("Use a provided custom delimiter for detecting the end of a line rather than a single line break.").
				Optional(),
			service.NewIntField(slFieldMaxBufferSize).
				Description("Set the maximum buffer size for storing line data, this limits the maximum size that a line can be without causing an error.").
				Default(bufio.MaxScanTokenSize),
		)
}

func init() {
	err := service.RegisterBatchScannerCreator("lines", linesScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return linesScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func linesScannerFromParsed(conf *service.ParsedConfig) (l *linesScanner, err error) {
	l = &linesScanner{}
	if conf.Contains(slFieldCustomDelimiter) {
		if l.customDelim, err = conf.FieldString(slFieldCustomDelimiter); err != nil {
			return
		}
	}
	if l.maxScanTokenSize, err = conf.FieldInt(slFieldMaxBufferSize); err != nil {
		return
	}
	return
}

type linesScanner struct {
	maxScanTokenSize int
	customDelim      string
}

func (l *linesScanner) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	scanner := bufio.NewScanner(rdr)
	if l.maxScanTokenSize != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, l.maxScanTokenSize)
	}

	if l.customDelim != "" {
		delimBytes := []byte(l.customDelim)
		scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}

			if i := bytes.Index(data, delimBytes); i >= 0 {
				// We have a full terminated line.
				return i + len(delimBytes), data[0:i], nil
			}

			// If we're at EOF, we have a final, non-terminated line. Return it.
			if atEOF {
				return len(data), data, nil
			}

			// Request more data.
			return 0, nil, nil
		})
	}

	return service.AutoAggregateBatchScannerAcks(&linesReaderStream{
		buf: scanner,
		r:   rdr,
	}, aFn), nil
}

func (l *linesScanner) Close(context.Context) error {
	return nil
}

type linesReaderStream struct {
	buf *bufio.Scanner
	r   io.ReadCloser
}

func (l *linesReaderStream) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	scanned := l.buf.Scan()
	if scanned {
		bytesCopy := make([]byte, len(l.buf.Bytes()))
		copy(bytesCopy, l.buf.Bytes())
		return service.MessageBatch{service.NewMessage(bytesCopy)}, nil
	}

	err := l.buf.Err()
	if err == nil {
		err = io.EOF
	}
	return nil, err
}

func (l *linesReaderStream) Close(ctx context.Context) error {
	return l.r.Close()
}
