// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package openai

import (
	"context"
	"fmt"
	"io"
	"slices"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	ospFieldInput          = "input"
	ospFieldVoice          = "voice"
	ospFieldResponseFormat = "response_format"
)

func init() {
	err := service.RegisterProcessor(
		"openai_speech",
		speechProcessorConfig(),
		makeSpeechProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func speechProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Processor that uses the OpenAI API to generate audio from the input text.").
		Description(`
This processor calls the OpenAI API for each message, creating speech. By default the entire message's payload as a string is submitted, and the `+"`"+ospFieldInput+"`"+` configuration field allows customizing that.

You can learn more about text-to-speech here: https://platform.openai.com/docs/guides/text-to-speech[https://platform.openai.com/docs/guides/text-to-speech^]`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"tts-1",
				"tts-1-hd",
			)...,
		).
		Fields(
			service.NewBloblangField(ospFieldInput).
				Description("The text to generate audio for. The maximum length is 4096 characters.").
				Optional(),
			service.NewInterpolatedStringField(ospFieldVoice).
				Description("The voice to use when generating the audio.").
				Examples("alloy", "echo", "fable", "onyx", "nova", "shimmer"),
			service.NewInterpolatedStringField(ospFieldResponseFormat).
				Description("The format to generate audio in. The default is mp3.").
				Examples("mp3", "opus", "aac", "flac", "wav", "pcm").
				Advanced().
				Optional(),
		)
}

func makeSpeechProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var i *bloblang.Executor
	if conf.Contains(ospFieldInput) {
		i, err = conf.FieldBloblang(ospFieldInput)
		if err != nil {
			return nil, err
		}
	}
	v, err := conf.FieldInterpolatedString(ospFieldVoice)
	if err != nil {
		return nil, err
	}
	var rf *service.InterpolatedString
	if conf.Contains(ospFieldResponseFormat) {
		rf, err = conf.FieldInterpolatedString(ospFieldResponseFormat)
		if err != nil {
			return nil, err
		}
	}
	return &speechProcessor{b, i, v, rf}, nil
}

type speechProcessor struct {
	*baseProcessor

	input          *bloblang.Executor
	voice          *service.InterpolatedString
	responseFormat *service.InterpolatedString
}

func (p *speechProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.SpeechGenerationOptions
	body.DeploymentName = &p.model
	v, err := p.voice.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("%s interpolation error: %w", ospFieldVoice, err)
	}
	voice := oai.SpeechVoice(v)
	if !slices.Contains(oai.PossibleSpeechVoiceValues(), voice) {
		return nil, fmt.Errorf("unknown speech voice value: %q", voice)
	}
	body.Voice = &voice
	if p.input != nil {
		v, err := msg.BloblangQueryValue(p.input)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", ospFieldInput, err)
		}
		s := bloblang.ValueToString(v)
		body.Input = &s
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		s := string(b)
		body.Input = &s
	}
	if p.responseFormat != nil {
		rf, err := p.responseFormat.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", ospFieldResponseFormat, err)
		}
		format := oai.SpeechGenerationResponseFormat(rf)
		if !slices.Contains(oai.PossibleSpeechGenerationResponseFormatValues(), format) {
			return nil, fmt.Errorf("unknown speech generation format value: %q", format)
		}
		body.ResponseFormat = &format
	}
	var opts oai.GenerateSpeechFromTextOptions
	resp, err := p.client.GenerateSpeechFromText(ctx, body, &opts)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	msg = msg.Copy()
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}
