// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package openai

import (
	"context"
	"errors"
	"fmt"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	otlpFieldFile   = "file"
	otlpFieldPrompt = "prompt"
)

func init() {
	err := service.RegisterProcessor(
		"openai_translation",
		translationProcessorConfig(),
		makeTranslationProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func translationProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Translates spoken audio into English, using the OpenAI API.").
		Description(`
This processor sends an audio file object to OpenAI API to generate a translation. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+otlpFieldFile+"`"+` configuration field to customize it.

To learn more about translation, see the https://platform.openai.com/docs/guides/speech-to-text[OpenAI API documentation^]`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"whisper-1",
			)...,
		).
		Fields(
			service.NewBloblangField(otlpFieldFile).
				Description("The audio file object (not file name) to translate, in one of the following formats: `flac`, `mp3`, `mp4`, `mpeg`, `mpga`, `m4a`, `ogg`, `wav`, or `webm`.").
				Optional(),
			service.NewInterpolatedStringField(otlpFieldPrompt).
				Description("Optional text to guide the model's style or continue a previous audio segment. The prompt should match the audio language.").
				Optional().
				Advanced(),
		)
}

func makeTranslationProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var f *bloblang.Executor
	if conf.Contains(otlpFieldFile) {
		f, err = conf.FieldBloblang(otlpFieldFile)
		if err != nil {
			return nil, err
		}
	}
	var p *service.InterpolatedString
	if conf.Contains(otlpFieldPrompt) {
		p, err = conf.FieldInterpolatedString(otlpFieldPrompt)
		if err != nil {
			return nil, err
		}
	}
	return &translationProcessor{b, f, p}, nil
}

type translationProcessor struct {
	*baseProcessor

	file   *bloblang.Executor
	prompt *service.InterpolatedString
}

func (p *translationProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.AudioTranslationOptions
	body.DeploymentName = &p.model
	if p.file != nil {
		f, err := msg.BloblangQueryValue(p.file)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", otlpFieldFile, err)
		}
		body.File, err = bloblang.ValueAsBytes(f)
		if err != nil {
			return nil, fmt.Errorf("%s conversion error: %w", otlpFieldFile, err)
		}
	} else {
		f, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		body.File = f
	}
	if p.prompt != nil {
		pr, err := p.prompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", otlpFieldPrompt, err)
		}
		body.Prompt = &pr
	}
	var opts oai.GetAudioTranslationOptions
	resp, err := p.client.GetAudioTranslation(ctx, body, &opts)
	if err != nil {
		return nil, err
	}
	if resp.Text == nil {
		return nil, errors.New("missing text in translation response")
	}
	msg = msg.Copy()
	msg.SetBytes([]byte(*resp.Text))
	return service.MessageBatch{msg}, nil
}
