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
	"encoding/base64"
	"errors"
	"fmt"
	"slices"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	oipFieldPrompt  = "prompt"
	oipFieldQuality = "quality"
	oipFieldSize    = "size"
	oipFieldStyle   = "style"
)

func init() {
	err := service.RegisterProcessor(
		"openai_image_generation",
		imageProcessorConfig(),
		makeImageProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func imageProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Processor that uses the OpenAI API to generate an image.").
		Description(`
This processor calls the OpenAI API, generating an image. By default the entire message's payload as a string is submitted, and the `+"`"+oipFieldPrompt+"`"+` configuration field allows customizing that.

You can learn more about chat completion here: https://platform.openai.com/docs/guides/chat-completions[https://platform.openai.com/docs/guides/chat-completions^]`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"dall-e-3",
				"dall-e-2",
			)...,
		).
		Fields(
			service.NewBloblangField(oipFieldPrompt).
				Description("A text description of the desired image(s). The maximum length is 1000 characters for dall-e-2 and 4000 characters for dall-e-3.").
				Optional(),
			service.NewInterpolatedStringField(oipFieldQuality).
				Description("The quality of the image that will be generated. hd creates images with finer details and greater consistency across the image. This param is only supported for dall-e-3.").
				Examples("standard", "hd").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(oipFieldSize).
				Description("The size of the generated images. Must be one of 256x256, 512x512, or 1024x1024 for dall-e-2. Must be one of 1024x1024, 1792x1024, or 1024x1792 for dall-e-3 models.").
				Examples("1024x1024", "512x512", "1792x1024", "1024x1792").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(oipFieldStyle).
				Description("The style of the generated images. Must be one of vivid or natural. Vivid causes the model to lean towards generating hyper-real and dramatic images. Natural causes the model to produce more natural, less hyper-real looking images. This param is only supported for dall-e-3.").
				Examples("vivid", "natural").
				Advanced().
				Optional(),
		)
}

func makeImageProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	b, err := newBaseProcessor(conf)
	if err != nil {
		return nil, err
	}
	var i *bloblang.Executor
	if conf.Contains(oipFieldPrompt) {
		i, err = conf.FieldBloblang(oipFieldPrompt)
		if err != nil {
			return nil, err
		}
	}
	var q *service.InterpolatedString
	if conf.Contains(oipFieldQuality) {
		q, err = conf.FieldInterpolatedString(oipFieldQuality)
		if err != nil {
			return nil, err
		}
	}
	var style *service.InterpolatedString
	if conf.Contains(oipFieldStyle) {
		q, err = conf.FieldInterpolatedString(oipFieldStyle)
		if err != nil {
			return nil, err
		}
	}
	var size *service.InterpolatedString
	if conf.Contains(oipFieldSize) {
		q, err = conf.FieldInterpolatedString(oipFieldSize)
		if err != nil {
			return nil, err
		}
	}
	return &moderationProcessor{b, i, q, style, size}, nil
}

type moderationProcessor struct {
	*baseProcessor

	input   *bloblang.Executor
	quality *service.InterpolatedString
	style   *service.InterpolatedString
	size    *service.InterpolatedString
}

func (p *moderationProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var body oai.ImageGenerationOptions
	body.DeploymentName = &p.model
	format := oai.ImageGenerationResponseFormatBase64
	body.ResponseFormat = &format
	if p.input != nil {
		v, err := msg.BloblangQueryValue(p.input)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", oipFieldPrompt, err)
		}
		s := bloblang.ValueToString(v)
		body.Prompt = &s
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		s := string(b)
		body.Prompt = &s
	}
	if p.quality != nil {
		r, err := p.quality.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldQuality, err)
		}
		q := oai.ImageGenerationQuality(r)
		if !slices.Contains(oai.PossibleImageGenerationQualityValues(), q) {
			return nil, fmt.Errorf("invalid image quality: %q", q)
		}
		body.Quality = &q
	}
	if p.style != nil {
		r, err := p.style.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldStyle, err)
		}
		s := oai.ImageGenerationStyle(r)
		if !slices.Contains(oai.PossibleImageGenerationStyleValues(), s) {
			return nil, fmt.Errorf("invalid image style: %q", s)
		}
		body.Style = &s
	}
	if p.size != nil {
		r, err := p.size.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldSize, err)
		}
		s := oai.ImageSize(r)
		if !slices.Contains(oai.PossibleImageSizeValues(), s) {
			return nil, fmt.Errorf("invalid image style: %q", s)
		}
		body.Size = &s
	}
	var opts oai.GetImageGenerationsOptions
	resp, err := p.client.GetImageGenerations(ctx, body, &opts)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) != 1 {
		return nil, fmt.Errorf("expected single generated image in response, got: %d", len(resp.Data))
	}
	if resp.Data[0].Base64Data == nil {
		return nil, errors.New("missing generated image data in response")
	}
	b, err := base64.StdEncoding.DecodeString(*resp.Data[0].Base64Data)
	if err != nil {
		return nil, err
	}
	msg = msg.Copy()
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}
