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
	"encoding/base64"
	"errors"
	"fmt"

	oai "github.com/sashabaranov/go-openai"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	oipFieldPrompt  = "prompt"
	oipFieldQuality = "quality"
	oipFieldSize    = "size"
	oipFieldStyle   = "style"
)

func init() {
	service.MustRegisterProcessor(
		"openai_image_generation",
		imageProcessorConfig(),
		makeImageProcessor,
	)
}

func imageProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Generates an image from a text description and other attributes, using OpenAI API.").
		Description(`
This processor sends an image description and other attributes, such as image size and quality to the OpenAI API, which generates an image. By default, the processor submits the entire payload of each message as a string, unless you use the `+"`"+oipFieldPrompt+"`"+` configuration field to customize it.

To learn more about image generation, see the https://platform.openai.com/docs/guides/images[OpenAI API documentation^].`).
		Version("4.32.0").
		Fields(
			baseConfigFieldsWithModels(
				"dall-e-3",
				"dall-e-2",
			)...,
		).
		Fields(
			service.NewBloblangField(oipFieldPrompt).
				Description("A text description of the image you want to generate. The `prompt` field accepts a maximum of 1000 characters for `dall-e-2` and 4000 characters for `dall-e-3`.").
				Optional(),
			service.NewInterpolatedStringField(oipFieldQuality).
				Description("The quality of the image to generate. Use `hd` to create images with finer details and greater consistency across the image. This parameter is only supported for `dall-e-3` models.").
				Examples("standard", "hd").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(oipFieldSize).
				Description("The size of the generated image. Choose from `256x256`, `512x512`, or `1024x1024` for `dall-e-2`. Choose from `1024x1024`, `1792x1024`, or `1024x1792` for `dall-e-3` models.").
				Examples("1024x1024", "512x512", "1792x1024", "1024x1792").
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(oipFieldStyle).
				Description("The style of the generated image. Choose from `vivid` or `natural`. Vivid causes the model to lean towards generating hyperreal and dramatic images. Natural causes the model to produce more natural, less hyperreal looking images. This parameter is only supported for `dall-e-3`.").
				Examples("vivid", "natural").
				Advanced().
				Optional(),
		)
}

func makeImageProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

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
	var body oai.ImageRequest
	body.Model = p.model
	body.ResponseFormat = "b64_json"
	if p.input != nil {
		v, err := msg.BloblangQuery(p.input)
		if err != nil {
			return nil, fmt.Errorf("%s execution error: %w", oipFieldPrompt, err)
		}
		r, err := v.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("%s conversion error: %w", oipFieldPrompt, err)
		}
		body.Prompt = string(r)
	} else {
		b, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		s := string(b)
		body.Prompt = s
	}
	if p.quality != nil {
		r, err := p.quality.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldQuality, err)
		}
		body.Quality = r
	}
	if p.style != nil {
		r, err := p.style.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldStyle, err)
		}
		body.Style = r
	}
	if p.size != nil {
		r, err := p.size.TryString(msg)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", oipFieldSize, err)
		}
		body.Size = r
	}
	resp, err := p.client.CreateImage(ctx, body)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) != 1 {
		return nil, fmt.Errorf("expected single generated image in response, got: %d", len(resp.Data))
	}
	if resp.Data[0].B64JSON == "" {
		return nil, errors.New("missing generated image data in response")
	}
	b, err := base64.StdEncoding.DecodeString(resp.Data[0].B64JSON)
	if err != nil {
		return nil, err
	}
	msg = msg.Copy()
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}
