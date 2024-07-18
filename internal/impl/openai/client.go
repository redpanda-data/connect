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
	"errors"

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
