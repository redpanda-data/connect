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

	oai "github.com/sashabaranov/go-openai"
)

// A mockable client for unit testing
type client interface {
	CreateChatCompletion(ctx context.Context, body oai.ChatCompletionRequest) (oai.ChatCompletionResponse, error)
	CreateEmbeddings(ctx context.Context, body oai.EmbeddingRequestConverter) (oai.EmbeddingResponse, error)
	CreateSpeech(ctx context.Context, body oai.CreateSpeechRequest) (oai.RawResponse, error)
	CreateTranscription(ctx context.Context, body oai.AudioRequest) (oai.AudioResponse, error)
	CreateTranslation(ctx context.Context, body oai.AudioRequest) (oai.AudioResponse, error)
	CreateImage(ctx context.Context, body oai.ImageRequest) (oai.ImageResponse, error)
}
