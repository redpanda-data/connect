package pure

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func archiveProcConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing", "Utility").
		Summary("Archives all the messages of a batch into a single message according to the selected archive format.").
		Description(`
Some archive formats (such as tar, zip) treat each archive item (message part) as a file with a path. Since message parts only contain raw data a unique path must be generated for each part. This can be done by using function interpolations on the 'path' field as described [here](/docs/configuration/interpolation#bloblang-queries). For types that aren't file based (such as binary) the file field is ignored.

The resulting archived message adopts the metadata of the _first_ message part of the batch.

The functionality of this processor depends on being applied across messages that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`).
		Field(service.NewStringAnnotatedEnumField("format", map[string]string{
			`concatenate`: `Join the raw contents of each message into a single binary message.`,
			`tar`:         `Archive messages to a unix standard tape archive.`,
			`zip`:         `Archive messages to a zip file.`,
			`binary`:      `Archive messages to a [binary blob format](https://github.com/benthosdev/benthos/blob/main/internal/message/message.go#L96).`,
			`lines`:       `Join the raw contents of each message and insert a line break between each one.`,
			`json_array`:  `Attempt to parse each message as a JSON document and append the result to an array, which becomes the contents of the resulting message.`,
		}).Description("The archiving format to apply.")).
		Field(service.NewInterpolatedStringField("path").
			Description("The path to set for each message in the archive (when applicable).").
			Example("${!count(\"files\")}-${!timestamp_unix_nano()}.txt").
			Example("${!meta(\"kafka_key\")}-${!json(\"id\")}.json").
			Default("")).
		Example("Tar Archive", `
If we had JSON messages in a batch each of the form:

`+"```json"+`
{"doc":{"id":"foo","body":"hello world 1"}}
`+"```"+`

And we wished to tar archive them, setting their filenames to their respective unique IDs (with the extension `+"`.json`"+`), our config might look like
this:`, `
pipeline:
  processors:
    - archive:
        format: tar
        path: ${!json("doc.id")}.json
`)
}

func init() {
	err := service.RegisterBatchProcessor(
		"archive", archiveProcConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newArchiveFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type archiveFunc func(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error)

type headerFunc func(index int, body *service.Message) os.FileInfo

func tarArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)

	for i, part := range msg {
		hdr, err := tar.FileInfoHeader(hFunc(i, part), "")
		if err != nil {
			return nil, err
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		pBytes, err := part.AsBytes()
		if err != nil {
			return nil, err
		}
		if _, err := tw.Write(pBytes); err != nil {
			return nil, err
		}
	}

	tw.Close()
	msg[0].SetBytes(buf.Bytes())
	return msg[0], nil
}

func zipArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)

	for i, part := range msg {
		h, err := zip.FileInfoHeader(hFunc(i, part))
		if err != nil {
			return nil, err
		}
		h.Method = zip.Deflate

		w, err := zw.CreateHeader(h)
		if err != nil {
			return nil, err
		}

		pBytes, err := part.AsBytes()
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(pBytes); err != nil {
			return nil, err
		}
	}
	zw.Close()

	msg[0].SetBytes(buf.Bytes())
	return msg[0], nil
}

func binaryArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	parts := make([][]byte, 0, len(msg))
	for _, p := range msg {
		pBytes, err := p.AsBytes()
		if err != nil {
			return nil, err
		}
		parts = append(parts, pBytes)
	}

	msg[0].SetBytes(message.SerializeBytes(parts))
	return msg[0], nil
}

func linesArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	tmpParts := make([][]byte, len(msg))
	for i, part := range msg {
		var err error
		if tmpParts[i], err = part.AsBytes(); err != nil {
			return nil, err
		}
	}
	msg[0].SetBytes(bytes.Join(tmpParts, []byte("\n")))
	return msg[0], nil
}

func concatenateArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	var buf bytes.Buffer
	for _, part := range msg {
		pBytes, err := part.AsBytes()
		if err != nil {
			return nil, err
		}
		_, _ = buf.Write(pBytes)
	}
	msg[0].SetBytes(buf.Bytes())
	return msg[0], nil
}

func jsonArrayArchive(hFunc headerFunc, msg service.MessageBatch) (*service.Message, error) {
	var array []any

	for _, part := range msg {
		doc, jerr := part.AsStructuredMut()
		if jerr != nil {
			return nil, fmt.Errorf("failed to parse message as JSON: %v", jerr)
		}
		array = append(array, doc)
	}
	msg[0].SetStructuredMut(array)
	return msg[0], nil
}

func strToArchiver(str string) (archiveFunc, error) {
	switch str {
	case "tar":
		return tarArchive, nil
	case "zip":
		return zipArchive, nil
	case "binary":
		return binaryArchive, nil
	case "lines":
		return linesArchive, nil
	case "json_array":
		return jsonArrayArchive, nil
	case "concatenate":
		return concatenateArchive, nil
	}
	return nil, fmt.Errorf("archive format not recognised: %v", str)
}

//------------------------------------------------------------------------------

type archive struct {
	archive archiveFunc
	path    *service.InterpolatedString
	log     *service.Logger
}

func newArchiveFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*archive, error) {
	formatStr, err := conf.FieldString("format")
	if err != nil {
		return nil, err
	}
	pathStr, err := conf.FieldInterpolatedString("path")
	if err != nil {
		return nil, err
	}
	return newArchive(mgr, formatStr, pathStr)
}

func newArchive(nm *service.Resources, format string, path *service.InterpolatedString) (*archive, error) {
	archiver, err := strToArchiver(format)
	if err != nil {
		return nil, err
	}
	return &archive{
		archive: archiver,
		path:    path,
		log:     nm.Logger(),
	}, nil
}

//------------------------------------------------------------------------------

type fakeInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (f fakeInfo) Name() string {
	return f.name
}

func (f fakeInfo) Size() int64 {
	return f.size
}

func (f fakeInfo) Mode() os.FileMode {
	return f.mode
}

func (f fakeInfo) ModTime() time.Time {
	return time.Now()
}

func (f fakeInfo) IsDir() bool {
	return false
}

func (f fakeInfo) Sys() any {
	return nil
}

func (d *archive) createHeaderFunc(msg service.MessageBatch) func(int, *service.Message) os.FileInfo {
	return func(index int, body *service.Message) os.FileInfo {
		bBytes, _ := body.AsBytes()
		name, err := msg.TryInterpolatedString(index, d.path)
		if err != nil {
			d.log.Errorf("Name interpolation error: %w", err)
		}
		return fakeInfo{
			name: name,
			size: int64(len(bBytes)),
			mode: 0o666,
		}
	}
}

//------------------------------------------------------------------------------

func (d *archive) ProcessBatch(ctx context.Context, msg service.MessageBatch) ([]service.MessageBatch, error) {
	if len(msg) == 0 {
		return nil, nil
	}

	newPart, err := d.archive(d.createHeaderFunc(msg), msg)
	if err != nil {
		d.log.Errorf("Failed to create archive: %v\n", err)
		return nil, err
	}

	newPart = newPart.WithContext(batch.CtxWithCollapsedCount(newPart.Context(), len(msg)))
	return []service.MessageBatch{{newPart}}, nil
}

func (d *archive) Close(context.Context) error {
	return nil
}
