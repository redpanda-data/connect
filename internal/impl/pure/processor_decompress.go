package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newDecompress(conf.Decompress, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("decompress", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "decompress",
		Categories: []string{
			"Parsing",
		},
		Summary: `
Decompresses messages according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate, snappy, lz4.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", "The decompression algorithm to use.").HasOptions("gzip", "zlib", "bzip2", "flate", "snappy", "lz4"),
		).ChildDefaultAndTypesFromStruct(processor.NewDecompressConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type decompressProc struct {
	decomp DecompressFunc
	log    log.Modular
}

func newDecompress(conf processor.DecompressConfig, mgr bundle.NewManagement) (*decompressProc, error) {
	dcor, err := strToDecompressor(conf.Algorithm)
	if err != nil {
		return nil, err
	}
	return &decompressProc{
		decomp: dcor,
		log:    mgr.Logger(),
	}, nil
}

func (d *decompressProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newBytes, err := d.decomp(msg.AsBytes())
	if err != nil {
		d.log.Errorf("Failed to decompress message part: %v\n", err)
		return nil, err
	}

	msg.SetBytes(newBytes)
	return []*message.Part{msg}, nil
}

func (d *decompressProc) Close(context.Context) error {
	return nil
}
