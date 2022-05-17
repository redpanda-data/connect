package cue

import (
	"context"
	"fmt"
	"path/filepath"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"
	"github.com/benthosdev/benthos/v4/public/service"
	"go.uber.org/multierr"
)

type cueEvalProcessorConfig struct {
	dir      string
	pkg      string
	fillPath *service.InterpolatedString
	selector *service.InterpolatedString
}

func cueEvalProcessorConfigFromParsed(inConf *service.ParsedConfig) (conf cueEvalProcessorConfig, err error) {
	if conf.dir, err = inConf.FieldString("dir"); err != nil {
		return
	}
	if conf.pkg, err = inConf.FieldString("package"); err != nil {
		return
	}
	if inConf.Contains("fill_path") {
		if conf.fillPath, err = inConf.FieldInterpolatedString("fill_path"); err != nil {
			return
		}
	}
	if inConf.Contains("selector") {
		if conf.selector, err = inConf.FieldInterpolatedString("selector"); err != nil {
			return
		}
	}
	return
}

func newCUEEvalProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.2.0").
		Categories("Mapping").
		Summary("Evaluate and transform JSON or CUE messages using a CUE module.").
		Description(`
The evaluation of messages is performed in the scope of a package within a CUE module - or more accurately an [instance](https://cuelang.org/docs/concepts/packages/#instances) of a package. Messages can be either JSON or CUE encoded since CUE is a superset of JSON.
`[1:]).
		Field(service.NewStringField("dir").
			Default("").
			Description("The directory that contains the CUE module. This directory should contain the `cue.mod` subdirectory. An empty value means it will run in the current directory.")).
		Field(service.NewStringField("package").
			Default("").
			Description("Selects the package to load within the CUE module. If this is not set, then there should be only one package in the build context.")).
		Field(service.NewInterpolatedStringField("fill_path").
			Optional().
			Description("The path within the CUE instance to unify with the incoming message. This field supports interpolation.")).
		Field(service.NewInterpolatedStringField("selector").
			Optional().
			Description("A selector expression that is used to pluck a particular field's value after a CUE evaluation. This field supports interpolation."))
}

type cueEvalProcessorOptions struct {
	logger *service.Logger
}

type cueEvalProcessor struct {
	config   *cueEvalProcessorConfig
	context  *cue.Context
	instance *cue.Value
}

func newCUEEvalProcessor(inConf *service.ParsedConfig, options *cueEvalProcessorOptions) (*cueEvalProcessor, error) {
	config, err := cueEvalProcessorConfigFromParsed(inConf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	var rootDir string
	rootDir, err = filepath.Abs(config.dir)
	if err != nil {
		return nil, err
	}

	// This will create an instance of a desired package in a CUE module.
	// The CUE build tool gathers all the files that define a package and combines
	// them into what it calls an instance.
	binsts := load.Instances([]string{}, &load.Config{
		Dir:     rootDir,
		Package: config.pkg,
	})

	if len(binsts) > 1 {
		return nil, fmt.Errorf("expected 1 CUE instance but got %d instances", len(binsts))
	}

	instance := binsts[0]

	// An error can originate from a package itself or from one of the
	// package's dependencies.
	err = multierr.Append(
		cueErrorsToMultiErr(instance.Err),
		multierr.Combine(instance.DepsErrors...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load instances: %w", err)
	}

	cuectx := cuecontext.New()
	inst := cuectx.BuildInstance(instance)
	err = inst.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CUE context: %w", cueErrorsToMultiErr(err))
	}

	return &cueEvalProcessor{
		config:   &config,
		context:  cuectx,
		instance: &inst,
	}, nil
}

func (proc *cueEvalProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	outBatch := make(service.MessageBatch, 0, len(batch))

	for i, msg := range batch {
		outBatch = append(outBatch, msg)

		raw, err := msg.AsBytes()
		if err != nil {
			msg.SetError(err)
			continue
		}

		// support messages that contain either json or cue
		cueMsg := proc.context.CompileBytes(raw)
		err = cueMsg.Err()
		if err != nil {
			msg.SetError(err)
			continue
		}

		var fillPath cue.Path
		if proc.config.fillPath != nil {
			fillPath = cue.ParsePath(batch.InterpolatedString(i, proc.config.fillPath))
		}

		val := proc.instance.FillPath(fillPath, cueMsg)
		err = val.Err()
		if err != nil {
			msg.SetError(err)
			continue
		}

		var selector cue.Path
		if proc.config.selector != nil {
			selector = cue.ParsePath(batch.InterpolatedString(i, proc.config.selector))
		}

		val = val.LookupPath(selector)
		if err != nil {
			msg.SetError(err)
			continue
		}

		bs, err := val.MarshalJSON()
		if err != nil {
			msg.SetError(err)
			continue
		}

		msg.SetBytes(bs)
	}

	return []service.MessageBatch{outBatch}, nil
}

func (proc *cueEvalProcessor) Close(ctx context.Context) error {
	return nil
}

func init() {
	err := service.RegisterBatchProcessor(
		"cue_eval", newCUEEvalProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newCUEEvalProcessor(conf, &cueEvalProcessorOptions{
				logger: mgr.Logger(),
			})
		})

	if err != nil {
		panic(err)
	}
}
