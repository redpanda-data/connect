package pure

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func unarchiveProcConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing", "Utility").
		Summary("Unarchives messages according to the selected archive format into multiple messages within a [batch](/docs/configuration/batching).").
		Description(`
When a message is unarchived the new messages replace the original message in the batch. Messages that are selected but fail to unarchive (invalid format) will remain unchanged in the message batch but will be flagged as having failed, allowing you to [error handle them](/docs/configuration/error_handling).

## Metadata

The metadata found on the messages handled by this processor will be copied into the resulting messages. For the unarchive formats that contain file information (tar, zip), a metadata field is also added to each message called ` + "`archive_filename`" + ` with the extracted filename.
`).
		Field(service.NewStringAnnotatedEnumField("format", map[string]string{
			`tar`:            `Extract messages from a unix standard tape archive.`,
			`zip`:            `Extract messages from a zip file.`,
			`binary`:         `Extract messages from a [binary blob format](https://github.com/benthosdev/benthos/blob/main/internal/message/message.go#L96).`,
			`lines`:          `Extract the lines of a message each into their own message.`,
			`json_documents`: `Attempt to parse a message as a stream of concatenated JSON documents. Each parsed document is expanded into a new message.`,
			`json_array`:     `Attempt to parse a message as a JSON array, and extract each element into its own message.`,
			`json_map`:       `Attempt to parse the message as a JSON map and for each element of the map expands its contents into a new message. A metadata field is added to each message called ` + "`archive_key`" + ` with the relevant key from the top-level map.`,
			`csv`:            `Attempt to parse the message as a csv file (header required) and for each row in the file expands its contents into a json object in a new message.`,
			`csv:x`:          `Attempt to parse the message as a csv file (header required) and for each row in the file expands its contents into a json object in a new message using a custom delimiter. The custom delimiter must be a single character, e.g. the format "csv:\t" would consume a tab delimited file.`,
		}).Description("The unarchiving format to apply.").LintRule(``)) // NOTE: We disable the linter here because `csv:x` is a dynamic pattern
}

func init() {
	err := service.RegisterProcessor(
		"unarchive", unarchiveProcConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newUnarchiveFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type unarchiveFunc func(part *service.Message) (service.MessageBatch, error)

func tarUnarchive(part *service.Message) (service.MessageBatch, error) {
	pBytes, err := part.AsBytes()
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(pBytes)
	tr := tar.NewReader(buf)

	var newParts []*service.Message

	// Iterate through the files in the archive.
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
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
		newPart.SetBytes(newPartBuf.Bytes())
		newPart.MetaSet("archive_filename", h.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func zipUnarchive(part *service.Message) (service.MessageBatch, error) {
	pBytes, err := part.AsBytes()
	if err != nil {
		return nil, err
	}

	buf := bytes.NewReader(pBytes)
	zr, err := zip.NewReader(buf, int64(buf.Len()))
	if err != nil {
		return nil, err
	}

	var newParts service.MessageBatch

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
		newPart.SetBytes(newPartBuf.Bytes())
		newPart.MetaSet("archive_filename", f.Name)
		newParts = append(newParts, newPart)
	}

	return newParts, nil
}

func binaryUnarchive(part *service.Message) (service.MessageBatch, error) {
	pBytes, err := part.AsBytes()
	if err != nil {
		return nil, err
	}

	parts, err := message.DeserializeBytes(pBytes)
	if err != nil {
		return nil, err
	}

	batch := make(service.MessageBatch, len(parts))
	for i, p := range parts {
		batch[i] = part.Copy()
		batch[i].SetBytes(p)
	}
	return batch, nil
}

func linesUnarchive(part *service.Message) (service.MessageBatch, error) {
	pBytes, err := part.AsBytes()
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(pBytes, []byte("\n"))

	batch := make(service.MessageBatch, len(lines))
	for i, p := range lines {
		batch[i] = part.Copy()
		batch[i].SetBytes(p)
	}
	return batch, nil
}

func jsonDocumentsUnarchive(part *service.Message) (service.MessageBatch, error) {
	pBytes, err := part.AsBytes()
	if err != nil {
		return nil, err
	}

	var parts service.MessageBatch
	dec := json.NewDecoder(bytes.NewReader(pBytes))
	for {
		var m any
		if err := dec.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		newPart := part.Copy()
		newPart.SetStructuredMut(m)
		parts = append(parts, newPart)
	}
	return parts, nil
}

func jsonArrayUnarchive(part *service.Message) (service.MessageBatch, error) {
	jDoc, err := part.AsStructuredMut()
	if err != nil {
		return nil, fmt.Errorf("failed to parse message into JSON array: %v", err)
	}

	jArray, ok := jDoc.([]any)
	if !ok {
		return nil, fmt.Errorf("failed to parse message into JSON array: invalid type '%T'", jDoc)
	}

	parts := make(service.MessageBatch, len(jArray))
	for i, ele := range jArray {
		newPart := part.Copy()
		newPart.SetStructuredMut(ele)
		parts[i] = newPart
	}
	return parts, nil
}

func jsonMapUnarchive(part *service.Message) (service.MessageBatch, error) {
	jDoc, err := part.AsStructuredMut()
	if err != nil {
		return nil, fmt.Errorf("failed to parse message into JSON map: %v", err)
	}

	jMap, ok := jDoc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("failed to parse message into JSON map: invalid type '%T'", jDoc)
	}

	parts := make(service.MessageBatch, len(jMap))
	i := 0
	for key, ele := range jMap {
		newPart := part.Copy()
		newPart.SetStructuredMut(ele)
		newPart.MetaSet("archive_key", key)
		parts[i] = newPart
		i++
	}
	return parts, nil
}

func csvUnarchive(customComma *rune) func(*service.Message) (service.MessageBatch, error) {
	return func(part *service.Message) (service.MessageBatch, error) {
		pBytes, err := part.AsBytes()
		if err != nil {
			return nil, err
		}

		buf := bytes.NewReader(pBytes)

		scanner := csv.NewReader(buf)
		scanner.ReuseRecord = true
		if customComma != nil {
			scanner.Comma = *customComma
		}

		var newParts []*service.Message
		var headers []string

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

			obj := make(map[string]any, len(records))
			for i, r := range records {
				obj[headers[i]] = r
			}

			newPart := part.Copy()
			newPart.SetStructuredMut(obj)
			newParts = append(newParts, newPart)
		}

		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("failed to parse message as csv: %v", err)
		}

		return newParts, nil
	}
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
		return csvUnarchive(nil), nil
	}

	if strings.HasPrefix(str, "csv:") {
		by := strings.TrimPrefix(str, "csv:")
		if by == "" {
			return nil, errors.New("csv format requires a non-empty delimiter")
		}
		byRunes := []rune(by)
		if len(byRunes) != 1 {
			return nil, errors.New("csv format requires a single character delimiter")
		}
		byRune := byRunes[0]
		return csvUnarchive(&byRune), nil
	}

	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

type unarchiveProc struct {
	unarchive unarchiveFunc
	log       *service.Logger
}

func newUnarchiveFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*unarchiveProc, error) {
	formatStr, err := conf.FieldString("format")
	if err != nil {
		return nil, err
	}
	return newUnarchive(mgr, formatStr)
}

func newUnarchive(nm *service.Resources, format string) (*unarchiveProc, error) {
	unarchiver, err := strToUnarchiver(format)
	if err != nil {
		return nil, err
	}
	return &unarchiveProc{
		unarchive: unarchiver,
		log:       nm.Logger(),
	}, nil
}

func (d *unarchiveProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	newParts, err := d.unarchive(msg)
	if err != nil {
		d.log.Errorf("Failed to unarchive message part: %v\n", err)
		return nil, err
	}
	return newParts, nil
}

func (d *unarchiveProc) Close(context.Context) error {
	return nil
}
