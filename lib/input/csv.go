package input

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/filepath"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCSVFile] = TypeSpec{
		constructor: fromSimpleConstructor(NewCSVFile),
		Status:      docs.StatusStable,
		Summary:     "Reads one or more CSV files as structured records following the format described in RFC 4180.",
		FieldSpecs: docs.FieldSpecs{
			docs.FieldString(
				"paths", "A list of file paths to read from. Each file will be read sequentially until the list is exhausted, at which point the input will close. Glob patterns are supported, including super globs (double star).",
				[]string{"/tmp/foo.csv", "/tmp/bar/*.csv", "/tmp/data/**/*.csv"},
			).Array(),
			docs.FieldCommon("parse_header_row", "Whether to reference the first row as a header row. If set to true the output structure for messages will be an object where field keys are determined by the header row."),
			docs.FieldCommon("delimiter", `The delimiter to use for splitting values in each record, must be a single character.`),
			docs.FieldAdvanced("batch_count", `Optionally process records in batches. This can help to speed up the consumption of exceptionally large CSV files. When the end of the file is reached the remaining records are processed as a (potentially smaller) batch.`),
		},
		Description: `
This input offers more control over CSV parsing than the ` + "[`file` input](/docs/components/inputs/file)" + `.

When parsing with a header row each line of the file will be consumed as a structured object, where the key names are determined from the header now. For example, the following CSV file:

` + "```csv" + `
foo,bar,baz
first foo,first bar,first baz
second foo,second bar,second baz
` + "```" + `

Would produce the following messages:

` + "```json" + `
{"foo":"first foo","bar":"first bar","baz":"first baz"}
{"foo":"second foo","bar":"second bar","baz":"second baz"}
` + "```" + `

If, however, the field ` + "`parse_header_row` is set to `false`" + ` then arrays are produced instead, like follows:

` + "```json" + `
["first foo","first bar","first baz"]
["second foo","second bar","second baz"]
` + "```" + ``,
		Categories: []Category{
			CategoryLocal,
		},
		Footnotes: `
This input is particularly useful when consuming CSV from files too large to
parse entirely within memory. However, in cases where CSV is consumed from other
input types it's also possible to parse them using the
` + "[Bloblang `parse_csv` method](/docs/guides/bloblang/methods#parse_csv)" + `.`,
	}
}

//------------------------------------------------------------------------------

// CSVFileConfig contains configuration values for the CSVFile input type.
type CSVFileConfig struct {
	Paths          []string `json:"paths" yaml:"paths"`
	ParseHeaderRow bool     `json:"parse_header_row" yaml:"parse_header_row"`
	Delim          string   `json:"delimiter" yaml:"delimiter"`
	BatchCount     int      `json:"batch_count" yaml:"batch_count"`
}

// NewCSVFileConfig creates a new CSVFileConfig with default values.
func NewCSVFileConfig() CSVFileConfig {
	return CSVFileConfig{
		Paths:          []string{},
		ParseHeaderRow: true,
		Delim:          ",",
		BatchCount:     1,
	}
}

//------------------------------------------------------------------------------

// NewCSVFile creates a new CSV file input type.
func NewCSVFile(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	delimRunes := []rune(conf.CSVFile.Delim)
	if len(delimRunes) != 1 {
		return nil, errors.New("delimiter value must be exactly one character")
	}

	comma := delimRunes[0]

	pathsRemaining, err := filepath.Globs(conf.CSVFile.Paths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve path glob: %w", err)
	}
	if len(pathsRemaining) == 0 {
		return nil, errors.New("requires at least one input file path")
	}

	if conf.CSVFile.BatchCount < 1 {
		return nil, errors.New("batch_count must be at least 1")
	}

	rdr, err := newCSVReader(
		func(context.Context) (io.Reader, error) {
			if len(pathsRemaining) == 0 {
				return nil, io.EOF
			}

			path := pathsRemaining[0]
			handle, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			pathsRemaining = pathsRemaining[1:]

			return handle, nil
		},
		func(context.Context) {},
		optCSVSetComma(comma),
		optCSVSetExpectHeaders(conf.CSVFile.ParseHeaderRow),
		optCSVSetGroupCount(conf.CSVFile.BatchCount),
	)
	if err != nil {
		return nil, err
	}

	return NewAsyncReader(TypeFile, true, reader.NewAsyncPreserver(rdr), log, stats)
}

//------------------------------------------------------------------------------

// csvReader is an reader. implementation that consumes an io.Reader and parses
// it as a CSV file.
type csvReader struct {
	handleCtor func(ctx context.Context) (io.Reader, error)
	onClose    func(ctx context.Context)

	mut     sync.Mutex
	handle  io.Reader
	scanner *csv.Reader
	headers []string

	expectHeaders bool
	comma         rune
	strict        bool
	groupCount    int
}

// NewCSV creates a new reader input type able to create a feed of line
// delimited CSV records from an io.Reader.
//
// Callers must provide a constructor function for the target io.Reader, which
// is called on start up and again each time a reader is exhausted. If the
// constructor is called but there is no more content to create a Reader for
// then the error `io.EOF` should be returned and the CSV will close.
//
// Callers must also provide an onClose function, which will be called if the
// CSV has been instructed to shut down. This function should unblock any
// blocked Read calls.
func newCSVReader(
	handleCtor func(ctx context.Context) (io.Reader, error),
	onClose func(ctx context.Context),
	options ...func(r *csvReader),
) (*csvReader, error) {
	r := csvReader{
		handleCtor:    handleCtor,
		onClose:       onClose,
		comma:         ',',
		expectHeaders: true,
		strict:        false,
		groupCount:    1,
	}

	for _, opt := range options {
		opt(&r)
	}

	return &r, nil
}

//------------------------------------------------------------------------------

// OptCSVSetComma is a option func that sets the comma character (default ',')
// to be used to divide records.
func optCSVSetComma(comma rune) func(r *csvReader) {
	return func(r *csvReader) {
		r.comma = comma
	}
}

// OptCSVSetGroupCount is a option func that sets the group count used to batch
// process records.
func optCSVSetGroupCount(groupCount int) func(r *csvReader) {
	return func(r *csvReader) {
		r.groupCount = groupCount
	}
}

// OptCSVSetExpectHeaders is a option func that determines whether the first
// record from the CSV input outlines the names of columns.
func optCSVSetExpectHeaders(expect bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.expectHeaders = expect
	}
}

// OptCSVSetStrict is a option func that determines whether records with
// misaligned numbers of fields should be rejected.
func optCSVSetStrict(strict bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.strict = strict
	}
}

//------------------------------------------------------------------------------

func (r *csvReader) closeHandle() {
	if r.handle != nil {
		if closer, ok := r.handle.(io.ReadCloser); ok {
			closer.Close()
		}
		r.handle = nil
	}
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a new scanner for an io.Reader.
func (r *csvReader) ConnectWithContext(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	if r.scanner != nil {
		return nil
	}

	handle, err := r.handleCtor(ctx)
	if err != nil {
		if err == io.EOF {
			return component.ErrTypeClosed
		}
		return err
	}

	scanner := csv.NewReader(handle)
	scanner.Comma = r.comma
	scanner.ReuseRecord = true

	r.scanner = scanner
	r.handle = handle

	return nil
}

func (r *csvReader) readNext(reader *csv.Reader) ([]string, error) {
	records, err := reader.Read()
	if err != nil && (r.strict || len(records) == 0) {
		if err == io.EOF {
			r.mut.Lock()
			r.scanner = nil
			r.headers = nil
			r.mut.Unlock()
			return nil, component.ErrNotConnected
		}
		return nil, err
	}
	return records, nil
}

// ReadWithContext attempts to read a new line from the io.Reader.
func (r *csvReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	r.mut.Lock()
	scanner := r.scanner
	headers := r.headers
	r.mut.Unlock()

	if scanner == nil {
		return nil, nil, component.ErrNotConnected
	}

	msg := message.QuickBatch(nil)

	for i := 0; i < r.groupCount; i++ {
		records, err := r.readNext(scanner)
		if err != nil {
			if i == 0 {
				return nil, nil, err
			}
			break
		}

		if r.expectHeaders && headers == nil {
			headers = make([]string, 0, len(records))
			headers = append(headers, records...)

			r.mut.Lock()
			r.headers = headers
			r.mut.Unlock()

			if records, err = r.readNext(scanner); err != nil {
				return nil, nil, err
			}
		}

		part := message.NewPart(nil)

		var structured interface{}
		if len(headers) == 0 || len(headers) < len(records) {
			slice := make([]interface{}, 0, len(records))
			for _, r := range records {
				slice = append(slice, r)
			}
			structured = slice
		} else {
			obj := make(map[string]interface{}, len(records))
			for i, r := range records {
				obj[headers[i]] = r
			}
			structured = obj
		}

		if err := part.SetJSON(structured); err != nil {
			return nil, nil, err
		}
		msg.Append(part)
	}

	return msg, func(context.Context, response.Error) error { return nil }, nil
}

// CloseAsync shuts down the reader input and stops processing requests.
func (r *csvReader) CloseAsync() {
	go func() {
		r.mut.Lock()
		r.onClose(context.Background())
		r.closeHandle()
		r.mut.Unlock()
	}()
}

// WaitForClose blocks until the reader input has closed down.
func (r *csvReader) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
