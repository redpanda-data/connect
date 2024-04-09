package pure

import (
	"context"
	json "encoding/json"
	"errors"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	sjsonFieldContinueOnError = "continue_on_error"
)

func jsonDocumentScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consumes a stream of one or more JSON documents.").
		Fields(
			service.NewBoolField(sjsonFieldContinueOnError).
				Description("If a JSON decoding fails due to any error emit an empty message marked with the error and then continue consuming subsequent documents when possible.").
				Default(false),
		)
}

func init() {
	err := service.RegisterBatchScannerCreator("json_documents", jsonDocumentScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return jsonDocumentScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func jsonDocumentScannerFromParsed(conf *service.ParsedConfig) (l *jsonDocumentScannerCreator, err error) {
	l = &jsonDocumentScannerCreator{}
	if l.continueOnError, err = conf.FieldBool(sjsonFieldContinueOnError); err != nil {
		return
	}
	return
}

type jsonDocumentScannerCreator struct {
	continueOnError bool
}

func (js *jsonDocumentScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&jsonDocumentScanner{
		d:               json.NewDecoder(rdr),
		r:               rdr,
		continueOnError: js.continueOnError,
	}, aFn), nil
}

func (js *jsonDocumentScannerCreator) Close(context.Context) error {
	return nil
}

type jsonDocumentScanner struct {
	d *json.Decoder
	r io.ReadCloser

	continueOnError bool
}

func (js *jsonDocumentScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if js.r == nil {
		return nil, io.EOF
	}

	msg := service.NewMessage(nil)

	var jsonDocObj any

	if err := js.d.Decode(&jsonDocObj); err != nil {
		msg.SetError(err)
		if errors.Is(err, io.EOF) || !js.continueOnError {
			return nil, err
		}
	}

	// Decode will determine whether a the jsonObj is of type map[string]interface{} or []interface{}
	msg.SetStructuredMut(jsonDocObj)

	return service.MessageBatch{msg}, nil
}

func (js *jsonDocumentScanner) Close(ctx context.Context) error {
	if js.r == nil {
		return nil
	}
	return js.r.Close()
}
