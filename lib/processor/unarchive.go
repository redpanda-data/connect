package processor

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeUnarchive] = TypeSpec{
		constructor: NewUnarchive,
		Categories: []Category{
			CategoryParsing, CategoryUtility,
		},
		Summary: `
Unarchives messages according to the selected archive [format](#formats) into
multiple messages within a [batch](/docs/configuration/batching).`,
		Description: `
When a message is unarchived the new messages replace the original message in
the batch. Messages that are selected but fail to unarchive (invalid format)
will remain unchanged in the message batch but will be flagged as having failed,
allowing you to [error handle them](/docs/configuration/error_handling).

For the unarchive formats that contain file information (tar, zip), a metadata
field is added to each message called ` + "`archive_filename`" + ` with the
extracted filename.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("format", "The unarchive [format](#formats) to use.").HasOptions(
				"tar", "zip", "binary", "lines", "json_documents", "json_array", "json_map", "csv",
			),
			PartsFieldSpec,
		},
		Footnotes: `
## Formats

### ` + "`tar`" + `

Extract messages from a unix standard tape archive.

### ` + "`zip`" + `

Extract messages from a zip file.

### ` + "`binary`" + `

Extract messages from a binary blob format consisting of:

- Four bytes containing number of messages in the batch (in big endian)
- For each message part:
  + Four bytes containing the length of the message (in big endian)
  + The content of message

### ` + "`lines`" + `

Extract the lines of a message each into their own message.

### ` + "`json_documents`" + `

Attempt to parse a message as a stream of concatenated JSON documents. Each
parsed document is expanded into a new message.

### ` + "`json_array`" + `

Attempt to parse a message as a JSON array, and extract each element into its
own message.

### ` + "`json_map`" + `

Attempt to parse the message as a JSON map and for each element of the map
expands its contents into a new message. A metadata field is added to each
message called ` + "`archive_key`" + ` with the relevant key from the top-level
map.

### ` + "`csv`" + `

Attempt to parse the message as a csv file (header required) and for each row in 
the file expands its contents into a json object in a new message.`,
	}
}

//------------------------------------------------------------------------------

// UnarchiveConfig contains configuration fields for the Unarchive processor.
type UnarchiveConfig struct {
	Format string `json:"format" yaml:"format"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewUnarchiveConfig returns a UnarchiveConfig with default values.
func NewUnarchiveConfig() UnarchiveConfig {
	return UnarchiveConfig{
		// TODO: V4 change this default
		Format: "binary",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type unarchiveFunc func(part *message.Part) ([]*message.Part, error)

func tarUnarchive(part *message.Part) ([]*message.Part, error) {
	buf := bytes.NewBuffer(part.Get())
	tr := tar.NewReader(buf)

	var newParts []*message.Part

	// Iterate through the files in the archive.
	for {
		h, err := tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			return nil, err
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(tr); err != nil {
			return nil, err
		}

		newPart := part.Copy()
		newPart.Set(newPartBuf.Bytes())
		newPart.MetaSet("archive_filename", h.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func zipUnarchive(part *message.Part) ([]*message.Part, error) {
	buf := bytes.NewReader(part.Get())
	zr, err := zip.NewReader(buf, int64(buf.Len()))
	if err != nil {
		return nil, err
	}

	var newParts []*message.Part

	// Iterate through the files in the archive.
	for _, f := range zr.File {
		fr, err := f.Open()
		if err != nil {
			return nil, err
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(fr); err != nil {
			return nil, err
		}

		newPart := part.Copy()
		newPart.Set(newPartBuf.Bytes())
		newPart.MetaSet("archive_filename", f.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func binaryUnarchive(part *message.Part) ([]*message.Part, error) {
	msg, err := message.FromBytes(part.Get())
	if err != nil {
		return nil, err
	}
	parts := make([]*message.Part, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		newPart := part.Copy()
		newPart.Set(p.Get())
		parts[i] = newPart
		return nil
	})

	return parts, nil
}

func linesUnarchive(part *message.Part) ([]*message.Part, error) {
	lines := bytes.Split(part.Get(), []byte("\n"))
	parts := make([]*message.Part, len(lines))
	for i, l := range lines {
		newPart := part.Copy()
		newPart.Set(l)
		parts[i] = newPart
	}
	return parts, nil
}

func jsonDocumentsUnarchive(part *message.Part) ([]*message.Part, error) {
	var parts []*message.Part
	dec := json.NewDecoder(bytes.NewReader(part.Get()))
	for {
		var m interface{}
		if err := dec.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		newPart := part.Copy()
		if err := newPart.SetJSON(m); err != nil {
			return nil, fmt.Errorf("failed to set JSON contents of message: %v", err)
		}
		parts = append(parts, newPart)
	}
	return parts, nil
}

func jsonArrayUnarchive(part *message.Part) ([]*message.Part, error) {
	jDoc, err := part.JSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse message into JSON array: %v", err)
	}

	jArray, ok := jDoc.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse message into JSON array: invalid type '%T'", jDoc)
	}

	parts := make([]*message.Part, len(jArray))
	for i, ele := range jArray {
		newPart := part.Copy()
		if err = newPart.SetJSON(ele); err != nil {
			return nil, fmt.Errorf("failed to marshal element into new message: %v", err)
		}
		parts[i] = newPart
	}
	return parts, nil
}

func jsonMapUnarchive(part *message.Part) ([]*message.Part, error) {
	jDoc, err := part.JSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse message into JSON map: %v", err)
	}

	jMap, ok := jDoc.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to parse message into JSON map: invalid type '%T'", jDoc)
	}

	parts := make([]*message.Part, len(jMap))
	i := 0
	for key, ele := range jMap {
		newPart := part.Copy()
		if err = newPart.SetJSON(ele); err != nil {
			return nil, fmt.Errorf("failed to marshal element into new message: %v", err)
		}
		newPart.MetaSet("archive_key", key)
		parts[i] = newPart
		i++
	}
	return parts, nil
}

func csvUnarchive(part *message.Part) ([]*message.Part, error) {
	buf := bytes.NewReader(part.Get())

	scanner := csv.NewReader(buf)
	scanner.ReuseRecord = true

	var newParts []*message.Part

	var headers []string

	var err error

	for {
		var records []string
		records, err = scanner.Read()
		if err != nil {
			break
		}

		if headers == nil {
			headers = make([]string, len(records))
			copy(headers, records)
			continue
		}

		if len(records) < len(headers) {
			err = errors.New("row has too few values")
			break
		}

		if len(records) > len(headers) {
			err = errors.New("row has too many values")
			break
		}

		obj := make(map[string]interface{}, len(records))
		for i, r := range records {
			obj[headers[i]] = r
		}

		newPart := part.Copy()

		if err = newPart.SetJSON(obj); err != nil {
			err = fmt.Errorf("failed to set json on new part: %v", err)
			break
		}

		newParts = append(newParts, newPart)
	}

	if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to parse message as csv: %v", err)
	}

	return newParts, nil
}

func strToUnarchiver(str string) (unarchiveFunc, error) {
	switch str {
	case "tar":
		return tarUnarchive, nil
	case "zip":
		return zipUnarchive, nil
	case "binary":
		return binaryUnarchive, nil
	case "lines":
		return linesUnarchive, nil
	case "json_documents":
		return jsonDocumentsUnarchive, nil
	case "json_array":
		return jsonArrayUnarchive, nil
	case "json_map":
		return jsonMapUnarchive, nil
	case "csv":
		return csvUnarchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Unarchive is a processor that can selectively unarchive parts of a message
// following a chosen archive type.
type Unarchive struct {
	conf      UnarchiveConfig
	unarchive unarchiveFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewUnarchive returns a Unarchive processor.
func NewUnarchive(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	dcor, err := strToUnarchiver(conf.Unarchive.Format)
	if err != nil {
		return nil, err
	}
	return &Unarchive{
		conf:      conf.Unarchive,
		unarchive: dcor,
		log:       log,
		stats:     stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSkipped:   stats.GetCounter("skipped"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (d *Unarchive) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	d.mCount.Incr(1)

	newMsg := message.QuickBatch(nil)
	lParts := msg.Len()

	noParts := len(d.conf.Parts) == 0
	_ = msg.Iter(func(i int, part *message.Part) error {
		isTarget := noParts
		if !isTarget {
			nI := i - lParts
			for _, t := range d.conf.Parts {
				if t == nI || t == i {
					isTarget = true
					break
				}
			}
		}
		if !isTarget {
			newMsg.Append(msg.Get(i).Copy())
			return nil
		}

		span := tracing.CreateChildSpan(TypeUnarchive, part)
		defer span.Finish()

		newParts, err := d.unarchive(part)
		if err == nil {
			newMsg.Append(newParts...)
		} else {
			d.mErr.Incr(1)
			d.log.Errorf("Failed to unarchive message part: %v\n", err)
			newMsg.Append(part)
			FlagErr(newMsg.Get(-1), err)
			span.LogKV(
				"event", "error",
				"type", err.Error(),
			)
		}
		return nil
	})

	d.mBatchSent.Incr(1)
	d.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]*message.Batch{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (d *Unarchive) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (d *Unarchive) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
