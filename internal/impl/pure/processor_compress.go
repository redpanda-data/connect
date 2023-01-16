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
		p, err := newCompress(conf.Compress, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("compress", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "compress",
		Categories: []string{
			"Parsing",
		},
		Summary: `
Compresses messages according to the selected algorithm. Supported compression
algorithms are: gzip, zlib, flate, snappy, lz4.`,
		Description: `
The 'level' field might not apply to all algorithms.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("algorithm", "The compression algorithm to use.").HasOptions("gzip", "zlib", "flate", "snappy", "lz4"),
			docs.FieldInt("level", "The level of compression to use. May not be applicable to all algorithms."),
		).ChildDefaultAndTypesFromStruct(processor.NewCompressConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type compressProc struct {
	level int
	comp  CompressFunc
	log   log.Modular
}

func newCompress(conf processor.CompressConfig, mgr bundle.NewManagement) (*compressProc, error) {
	cor, err := strToCompressor(conf.Algorithm)
	if err != nil {
		return nil, err
	}
	return &compressProc{
		level: conf.Level,
		comp:  cor,
		log:   mgr.Logger(),
	}, nil
}

func (c *compressProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newBytes, err := c.comp(c.level, msg.AsBytes())
	if err != nil {
		c.log.Errorf("Failed to compress message: %v\n", err)
		return nil, err
	}
	msg.SetBytes(newBytes)
	return []*message.Part{msg}, nil
}

func (c *compressProc) Close(context.Context) error {
	return nil
}
