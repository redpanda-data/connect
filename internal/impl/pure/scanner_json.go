package pure

import (
	"context"
	"encoding/json"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

func jsonDocumentScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.27.0").
		Summary("Consumes a stream of one or more JSON documents.").
		// Just a placeholder empty object as we don't have any fields yet
		Field(service.NewObjectField("").Default(map[string]any{}))
}

func init() {
	err := service.RegisterBatchScannerCreator("json_documents", jsonDocumentScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return &jsonDocumentScannerCreator{}, nil
		})
	if err != nil {
		panic(err)
	}
}

type jsonDocumentScannerCreator struct{}

func (js *jsonDocumentScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&jsonDocumentScanner{
		d: json.NewDecoder(rdr),
		r: rdr,
	}, aFn), nil
}

func (js *jsonDocumentScannerCreator) Close(context.Context) error {
	return nil
}

type jsonDocumentScanner struct {
	d *json.Decoder
	r io.ReadCloser
}

func (js *jsonDocumentScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if js.r == nil {
		return nil, io.EOF
	}

	var jsonDocObj any
	if err := js.d.Decode(&jsonDocObj); err != nil {
		_ = js.r.Close()
		js.r = nil
		return nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(jsonDocObj)

	return service.MessageBatch{msg}, nil
}

func (js *jsonDocumentScanner) Close(ctx context.Context) error {
	if js.r == nil {
		return nil
	}
	return js.r.Close()
}
