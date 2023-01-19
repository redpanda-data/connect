package io

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type csvScannerInfo struct {
	handle      io.Reader
	deleteFn    func() error
	currentPath string
	modTimeUTC  time.Time
}

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		delimRunes := []rune(conf.CSVFile.Delim)
		if len(delimRunes) != 1 {
			return nil, errors.New("delimiter value must be exactly one character")
		}

		comma := delimRunes[0]

		pathsRemaining, err := filepath.Globs(nm.FS(), conf.CSVFile.Paths)
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
			func(context.Context) (csvScannerInfo, error) {
				if len(pathsRemaining) == 0 {
					return csvScannerInfo{}, io.EOF
				}

				path := pathsRemaining[0]
				handle, err := nm.FS().Open(path)
				if err != nil {
					return csvScannerInfo{}, err
				}

				var modTimeUTC time.Time
				if fInfo, err := handle.Stat(); err == nil {
					modTimeUTC = fInfo.ModTime().UTC()
				} else {
					nm.Logger().Errorf("Failed to read metadata from file '%v'", path)
				}

				pathsRemaining = pathsRemaining[1:]

				return csvScannerInfo{
					handle: handle,
					deleteFn: func() error {
						return nm.FS().Remove(path)
					},
					currentPath: path,
					modTimeUTC:  modTimeUTC,
				}, nil
			},
			func(context.Context) {},
			optCSVSetComma(comma),
			optCSVSetExpectHeader(conf.CSVFile.ParseHeaderRow),
			optCSVSetGroupCount(conf.CSVFile.BatchCount),
			optCSVSetLazyQuotes(conf.CSVFile.LazyQuotes),
			optCSVSetDeleteOnFinish(conf.CSVFile.DeleteOnFinish),
		)
		if err != nil {
			return nil, err
		}

		return input.NewAsyncReader("csv", input.NewAsyncPreserver(rdr), nm)
	}), docs.ComponentSpec{
		Name:    "csv",
		Status:  docs.StatusStable,
		Summary: "Reads one or more CSV files as structured records following the format described in RFC 4180.",
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"paths", "A list of file paths to read from. Each file will be read sequentially until the list is exhausted, at which point the input will close. Glob patterns are supported, including super globs (double star).",
				[]string{"/tmp/foo.csv", "/tmp/bar/*.csv", "/tmp/data/**/*.csv"},
			).Array(),
			docs.FieldBool("parse_header_row", "Whether to reference the first row as a header row. If set to true the output structure for messages will be an object where field keys are determined by the header row. Otherwise, each message will consist of an array of values from the corresponding CSV row."),
			docs.FieldString("delimiter", `The delimiter to use for splitting values in each record. It must be a single character.`),
			docs.FieldBool("lazy_quotes", "If set to `true`, a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field.").AtVersion("4.1.0"),
			docs.FieldBool("delete_on_finish", "Whether to delete input files from the disk once they are fully consumed.").Advanced(),
			docs.FieldInt("batch_count", `Optionally process records in batches. This can help to speed up the consumption of exceptionally large CSV files. When the end of the file is reached the remaining records are processed as a (potentially smaller) batch.`).Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewCSVFileConfig()),
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
` + "```" + `

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- header
- path
- mod_time_unix
- mod_time (RFC3339)
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

Note: The ` + "`header`" + ` field is only set when ` + "`parse_header_row`" + ` is ` + "`true`" + `.

### Output CSV column order

When [creating CSV](/docs/guides/bloblang/advanced#creating-csv) from Benthos messages, the columns must be sorted lexicographically to make the output deterministic. Alternatively, when using the ` + "`csv`" + ` input, one can leverage the ` + "`header`" + ` metadata field to retrieve the column order:

` + "```yaml" + `
input:
  csv:
    paths:
      - ./foo.csv
      - ./bar.csv
    parse_header_row: true

  processors:
    - mapping: |
        map escape_csv {
          root = if this.re_match("[\"\n,]+") {
            "\"" + this.replace_all("\"", "\"\"") + "\""
          } else {
            this
          }
        }

        let header = if count(@path) == 1 {
          @header.map_each(c -> c.apply("escape_csv")).join(",") + "\n"
        } else { "" }

        root = $header + @header.map_each(c -> this.get(c).string().apply("escape_csv")).join(",")

output:
  file:
    path: ./output/${! @path.filepath_split().index(-1) }
` + "```" + `
`,
		Categories: []string{
			"Local",
		},
		Footnotes: `
This input is particularly useful when consuming CSV from files too large to
parse entirely within memory. However, in cases where CSV is consumed from other
input types it's also possible to parse them using the
` + "[Bloblang `parse_csv` method](/docs/guides/bloblang/methods#parse_csv)" + `.`,
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type csvReader struct {
	handleCtor func(ctx context.Context) (csvScannerInfo, error)
	onClose    func(ctx context.Context)

	mut         sync.Mutex
	handle      io.Reader
	scanner     *csv.Reader
	scannerInfo csvScannerInfo
	header      []any

	expectHeader bool
	comma        rune
	strict       bool
	groupCount   int
	lazyQuotes   bool
	delete       bool
}

// newCSVReader creates a new reader input type able to create a feed of line
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
	handleCtor func(ctx context.Context) (csvScannerInfo, error),
	onClose func(ctx context.Context),
	options ...func(r *csvReader),
) (*csvReader, error) {
	r := csvReader{
		handleCtor:   handleCtor,
		onClose:      onClose,
		comma:        ',',
		expectHeader: true,
		strict:       false,
		groupCount:   1,
		lazyQuotes:   false,
		delete:       false,
	}

	for _, opt := range options {
		opt(&r)
	}

	return &r, nil
}

//------------------------------------------------------------------------------

// OptCSVSetComma is a option func that sets the comma character (default ',')
// to be used to divide record fields.
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

// OptCSVSetExpectHeader is an option func that determines whether the first
// record from the CSV input outlines the names of columns.
func optCSVSetExpectHeader(expect bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.expectHeader = expect
	}
}

// OptCSVSetStrict is an option func that determines whether records with
// misaligned numbers of fields should be rejected.
func optCSVSetStrict(strict bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.strict = strict
	}
}

// optCSVSetLazyQuotes is an option func that determines whether a quote may
// appear in an unquoted field and a non-doubled quote may appear in a quoted field.
func optCSVSetLazyQuotes(lazyQuotes bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.lazyQuotes = lazyQuotes
	}
}

// optCSVSetDeleteOnFinish is an option func that determines whether to delete
// consumed files from the disk once they are fully consumed.
func optCSVSetDeleteOnFinish(del bool) func(r *csvReader) {
	return func(r *csvReader) {
		r.delete = del
	}
}

//------------------------------------------------------------------------------

func (r *csvReader) closeHandle() (err error) {
	if r.handle != nil {
		if closer, ok := r.handle.(io.ReadCloser); ok {
			err = closer.Close()
		}
		r.handle = nil
	}
	return
}

func (r *csvReader) Connect(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	if r.scanner != nil {
		return nil
	}

	scannerInfo, err := r.handleCtor(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return component.ErrTypeClosed
		}
		return err
	}

	scanner := csv.NewReader(scannerInfo.handle)
	scanner.LazyQuotes = r.lazyQuotes
	scanner.Comma = r.comma
	scanner.ReuseRecord = true

	r.scanner = scanner
	r.scannerInfo = scannerInfo

	return nil
}

func (r *csvReader) readNext(reader *csv.Reader) ([]string, error) {
	record, err := reader.Read()
	if err != nil && (r.strict || len(record) == 0) {
		if errors.Is(err, io.EOF) {
			var deleteFn func() error
			r.mut.Lock()
			r.scanner = nil
			r.header = nil
			deleteFn = r.scannerInfo.deleteFn
			r.mut.Unlock()

			if r.delete {
				if err := deleteFn(); err != nil {
					return nil, err
				}
			}
			return nil, component.ErrNotConnected
		}
		return nil, err
	}
	return record, nil
}

func (r *csvReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	r.mut.Lock()
	scanner := r.scanner
	scannerInfo := r.scannerInfo
	header := r.header
	r.mut.Unlock()

	if scanner == nil {
		return nil, nil, component.ErrNotConnected
	}

	msg := message.QuickBatch(nil)

	for i := 0; i < r.groupCount; i++ {
		record, err := r.readNext(scanner)
		if err != nil {
			if i == 0 {
				return nil, nil, err
			}
			break
		}

		if r.expectHeader && header == nil {
			header = make([]any, 0, len(record))
			for _, rec := range record {
				header = append(header, rec)
			}

			r.mut.Lock()
			r.header = header
			r.mut.Unlock()

			if record, err = r.readNext(scanner); err != nil {
				return nil, nil, err
			}
		}

		part := message.NewPart(nil)

		var structured any
		if len(header) == 0 || len(header) < len(record) {
			slice := make([]any, 0, len(record))
			for _, r := range record {
				slice = append(slice, r)
			}
			structured = slice
		} else {
			obj := make(map[string]any, len(record))
			for i, r := range record {
				// The `header` slice contains only strings, but we define it as `[]any` so it resolves to a bloblang
				// array when we extract it from the metadata.
				obj[header[i].(string)] = r
			}
			structured = obj

			part.MetaSetMut("header", header)
		}

		part.MetaSetMut("path", scannerInfo.currentPath)
		part.MetaSetMut("mod_time_unix", scannerInfo.modTimeUTC.Unix())
		part.MetaSetMut("mod_time", scannerInfo.modTimeUTC.Format(time.RFC3339))

		part.SetStructuredMut(structured)
		msg = append(msg, part)
	}

	return msg, func(context.Context, error) error { return nil }, nil
}

func (r *csvReader) Close(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.onClose(ctx)
	return r.closeHandle()
}
