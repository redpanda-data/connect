// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package openai

import (
	"context"
	"errors"

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
)

type stubClient struct{}

func (*stubClient) GetEmbeddings(ctx context.Context, body oai.EmbeddingsOptions, options *oai.GetEmbeddingsOptions) (r oai.GetEmbeddingsResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) GetChatCompletions(ctx context.Context, body oai.ChatCompletionsOptions, options *oai.GetChatCompletionsOptions) (r oai.GetChatCompletionsResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) GenerateSpeechFromText(ctx context.Context, body oai.SpeechGenerationOptions, options *oai.GenerateSpeechFromTextOptions) (r oai.GenerateSpeechFromTextResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) GetAudioTranscription(ctx context.Context, body oai.AudioTranscriptionOptions, options *oai.GetAudioTranscriptionOptions) (r oai.GetAudioTranscriptionResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) GetAudioTranslation(ctx context.Context, body oai.AudioTranslationOptions, options *oai.GetAudioTranslationOptions) (r oai.GetAudioTranslationResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) GetImageGenerations(ctx context.Context, body oai.ImageGenerationOptions, options *oai.GetImageGenerationsOptions) (r oai.GetImageGenerationsResponse, err error) {
	err = errors.New("unimplemented")
	return
}
