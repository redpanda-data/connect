package pure

import (
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

func toTheEndScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Read the input stream all the way until the end and deliver it as a single message.").
		Description(`
:::caution
Some sources of data may not have a logical end, therefore caution should be made to exclusively use this scanner when the end of an input stream is clearly defined (and well within memory).
:::
`).
		Field(service.NewObjectField("").Default(map[string]any{}))
}

func init() {
	err := service.RegisterBatchScannerCreator("to_the_end", toTheEndScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return toTheEndScannerCreatorFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func toTheEndScannerCreatorFromParsed(conf *service.ParsedConfig) (s *toTheEndScannerCreator, err error) {
	s = &toTheEndScannerCreator{}
	return
}

type toTheEndScannerCreator struct{}

func (l *toTheEndScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&toTheEndScanner{r: rdr}, aFn), nil
}

func (l *toTheEndScannerCreator) Close(context.Context) error {
	return nil
}

type toTheEndScanner struct {
	r io.ReadCloser
}

func (t *toTheEndScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if t.r == nil {
		return nil, io.EOF
	}
	mBytes, err := io.ReadAll(t.r)
	if err != nil {
		return nil, err
	}
	_ = t.r.Close()
	t.r = nil
	return service.MessageBatch{service.NewMessage(mBytes)}, err
}

func (t *toTheEndScanner) Close(ctx context.Context) error {
	if t.r == nil {
		return nil
	}
	return t.r.Close()
}
