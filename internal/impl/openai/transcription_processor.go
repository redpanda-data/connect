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
	otspFieldFile   = "file"
	otspFieldLang   = "language"
	otspFieldPrompt = "prompt"
)

func init() {
	err := service.RegisterProcessor(
		"openai_transcription",
		transcriptionProcessorConfig(),
		makeTranscriptionProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func transcriptionProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates a transcription of spoken audio in the input language, using the OpenAI API.").
		Description(`
This processor sends an audio file object along with the input language to OpenAI API to generate a transcription. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+otspFieldFile+"`"+` configuration field to customize it.

To learn more about audio transcription, see the: https://platform.openai.com/docs/guides/speech-to-text[OpenAI API documentation^]`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"whisper-1",
			)...,
		).
		Fields(
			service.NewBloblangField(otspFieldFile).
				Description("The audio file object (not file name) to transcribe, in one of the following formats: `flac`, `mp3`, `mp4`, `mpeg`, `mpga`, `m4a`, `ogg`, `wav`, or `webm`."),
			service.NewInterpolatedStringField(otspFieldLang).
				Description("The language of the input audio. Supplying the input language in ISO-639-1 format improves accuracy and latency.").
				Examples("en", "fr", "de", "zh").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(otspFieldPrompt).
				Description("Optional text to guide the model's style or continue a previous audio segment. The prompt should match the audio language.").
				Optional().
				Advanced(),
		)
}

func makeTranscriptionProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	f, err := conf.FieldBloblang(otspFieldFile)
	if err != nil {
		return nil, err
	}
	var l *service.InterpolatedString
	if conf.Contains(otspFieldLang) {
		l, err = conf.FieldInterpolatedString(otspFieldLang)
		if err != nil {
			return nil, err
		}
	}
	var p *service.InterpolatedString
	if conf.Contains(otspFieldPrompt) {
		p, err = conf.FieldInterpolatedString(otspFieldPrompt)
		if err != nil {
			return nil, err
		}
	}
	return &transcriptionProcessor{b, f, l, p}, nil
}

type transcriptionProcessor struct {
	*baseProcessor

	file   *bloblang.Executor
	lang   *service.InterpolatedString
	prompt *service.InterpolatedString
}

func (p *transcriptionProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.AudioTranscriptionOptions
	body.DeploymentName = &p.model
	f, err := msg.BloblangQueryValue(p.file)
	if err != nil {
		return nil, fmt.Errorf("%s execution error: %w", otspFieldFile, err)
	}
	body.File, err = bloblang.ValueAsBytes(f)
	if err != nil {
		return nil, err
	}
	if p.lang != nil {
		l, err := p.lang.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", otspFieldLang, err)
		}
		body.Language = &l
	}
	if p.prompt != nil {
		pr, err := p.prompt.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", otspFieldPrompt, err)
		}
		body.Prompt = &pr
	}
	var opts oai.GetAudioTranscriptionOptions
	resp, err := p.client.GetAudioTranscription(ctx, body, &opts)
	if err != nil {
		return nil, err
	}
	if resp.Text == nil {
		return nil, errors.New("missing text in transcription response")
	}
	msg = msg.Copy()
	msg.SetBytes([]byte(*resp.Text))
	return service.MessageBatch{msg}, nil
}
