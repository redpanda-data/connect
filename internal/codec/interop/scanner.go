package interop

import (
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fieldCodecFromString = "codec"
	crFieldCodec         = "scanner"
	crFieldMaxBuffer     = "max_buffer"
)

func OldReaderCodecFields(defaultScanner string) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewInternalField(codec.NewReaderDocs(fieldCodecFromString)).Deprecated().Optional(),
		service.NewIntField(crFieldMaxBuffer).Deprecated().Default(1000000),
		service.NewScannerField(crFieldCodec).
			Description("The [scanner](/docs/components/scanners/about) by which the stream of bytes consumed will be broken out into individual messages. Scanners are useful for processing large sources of data without holding the entirety of it within memory. For example, the `csv` scanner allows you to process individual CSV rows without loading the entire CSV file in memory at once.").
			Default(map[string]any{defaultScanner: map[string]any{}}).
			Optional(),
	}
}

type FallbackReaderCodec interface {
	Create(rdr io.ReadCloser, aFn service.AckFunc, details scanner.SourceDetails) (FallbackReaderStream, error)
	Close(context.Context) error
}

type FallbackReaderStream interface {
	NextBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error)
	Close(context.Context) error
}

func OldReaderCodecFromParsed(conf *service.ParsedConfig) (FallbackReaderCodec, error) {
	if conf.Contains(fieldCodecFromString) {
		codecName, err := conf.FieldString(fieldCodecFromString)
		if err != nil {
			return nil, err
		}

		maxBuffer, err := conf.FieldInt(crFieldMaxBuffer)
		if err != nil {
			return nil, err
		}

		oldCtor, err := codec.GetReader(codecName, codec.ReaderConfig{
			MaxScanTokenSize: maxBuffer,
		})
		if err != nil {
			return nil, err
		}
		return &codecRInternal{oldCtor}, nil
	}

	ownedCodec, err := conf.FieldScanner(crFieldCodec)
	if err != nil {
		return nil, err
	}
	return &codecRPublic{newCtor: ownedCodec}, nil
}

type codecRInternal struct {
	oldCtor codec.ReaderConstructor
}

func (r *codecRInternal) Create(rdr io.ReadCloser, aFn service.AckFunc, details scanner.SourceDetails) (FallbackReaderStream, error) {
	oldR, err := r.oldCtor(details.Name, rdr, codec.ReaderAckFn(aFn))
	if err != nil {
		return nil, err
	}
	return &streamRInternal{oldR}, nil
}

func (r *codecRInternal) Close(ctx context.Context) error {
	return nil
}

type streamRInternal struct {
	old codec.Reader
}

func (r *streamRInternal) NextBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ib, aFn, err := r.old.Next(ctx)
	if err != nil {
		return nil, nil, err
	}

	batch := make(service.MessageBatch, len(ib))
	for i := range ib {
		batch[i] = service.NewInternalMessage(ib[i])
	}
	return batch, service.AckFunc(aFn), nil
}

func (r *streamRInternal) Close(ctx context.Context) error {
	return r.old.Close(ctx)
}

type codecRPublic struct {
	newCtor *service.OwnedScannerCreator
}

func (r *codecRPublic) Create(rdr io.ReadCloser, aFn service.AckFunc, details scanner.SourceDetails) (FallbackReaderStream, error) {
	sDetails := service.NewScannerSourceDetails()
	sDetails.SetName(details.Name)
	return r.newCtor.Create(rdr, aFn, sDetails)
}

func (r *codecRPublic) Close(ctx context.Context) error {
	return r.newCtor.Close(ctx)
}
