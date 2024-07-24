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

	oai "github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
)

// A mockable client for unit testing
type client interface {
	GetEmbeddings(ctx context.Context, body oai.EmbeddingsOptions, options *oai.GetEmbeddingsOptions) (oai.GetEmbeddingsResponse, error)
	GetChatCompletions(ctx context.Context, body oai.ChatCompletionsOptions, options *oai.GetChatCompletionsOptions) (oai.GetChatCompletionsResponse, error)
	GenerateSpeechFromText(ctx context.Context, body oai.SpeechGenerationOptions, options *oai.GenerateSpeechFromTextOptions) (oai.GenerateSpeechFromTextResponse, error)
	GetAudioTranscription(ctx context.Context, body oai.AudioTranscriptionOptions, options *oai.GetAudioTranscriptionOptions) (oai.GetAudioTranscriptionResponse, error)
	GetAudioTranslation(ctx context.Context, body oai.AudioTranslationOptions, options *oai.GetAudioTranslationOptions) (oai.GetAudioTranslationResponse, error)
	GetImageGenerations(ctx context.Context, body oai.ImageGenerationOptions, options *oai.GetImageGenerationsOptions) (oai.GetImageGenerationsResponse, error)
}
