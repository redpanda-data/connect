package pure

import (
	"bufio"
	"context"
	"io"
	"regexp"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	sremFieldPattern       = "pattern"
	sremFieldMaxBufferSize = "max_buffer_size"
)

func reMatchScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Split an input stream into segments matching against a regular expression.").
		Fields(
			service.NewStringField(sremFieldPattern).
				Description("The pattern to match against.").
				Example("(?m)^\\d\\d:\\d\\d:\\d\\d"),
			service.NewIntField(sremFieldMaxBufferSize).
				Description("Set the maximum buffer size for storing line data, this limits the maximum size that a message can be without causing an error.").
				Default(bufio.MaxScanTokenSize),
		)
}

func init() {
	err := service.RegisterBatchScannerCreator("re_match", reMatchScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return reMatchScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func reMatchScannerFromParsed(conf *service.ParsedConfig) (l *reMatchScanner, err error) {
	l = &reMatchScanner{}
	var regex string
	if regex, err = conf.FieldString(sremFieldPattern); err != nil {
		return
	}
	if l.maxScanTokenSize, err = conf.FieldInt(sremFieldMaxBufferSize); err != nil {
		return
	}

	if l.regex, err = regexp.Compile(regex); err != nil {
		return nil, err
	}
	return
}

type reMatchScanner struct {
	maxScanTokenSize int
	regex            *regexp.Regexp
}

func (l *reMatchScanner) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	scanner := bufio.NewScanner(rdr)
	if l.maxScanTokenSize != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, l.maxScanTokenSize)
	}

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		loc := l.regex.FindAllIndex(data, 2)
		if loc == nil {
			if atEOF {
				return len(data), data, nil
			}
			return 0, nil, nil
		}

		if len(loc) == 1 {
			if atEOF {
				if loc[0][0] == 0 {
					return len(data), data, nil
				}
				return loc[0][0], data[0:loc[0][0]], nil
			}
			return 0, nil, nil
		}
		if loc[0][0] == 0 {
			return loc[1][0], data[0:loc[1][0]], nil
		}
		return loc[0][0], data[0:loc[0][0]], nil
	})

	return service.AutoAggregateBatchScannerAcks(&reMatchReaderStream{
		buf: scanner,
		r:   rdr,
	}, aFn), nil
}

func (l *reMatchScanner) Close(context.Context) error {
	return nil
}

type reMatchReaderStream struct {
	buf *bufio.Scanner
	r   io.ReadCloser
}

func (l *reMatchReaderStream) NextBatch(ctx context.Context) (service.MessageBatch, error) {
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

func (l *reMatchReaderStream) Close(ctx context.Context) error {
	return l.r.Close()
}
