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

	oai "github.com/sashabaranov/go-openai"
)

type stubClient struct{}

func (*stubClient) CreateEmbeddings(ctx context.Context, body oai.EmbeddingRequestConverter) (r oai.EmbeddingResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateChatCompletion(ctx context.Context, body oai.ChatCompletionRequest) (r oai.ChatCompletionResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateSpeech(ctx context.Context, body oai.CreateSpeechRequest) (r oai.RawResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateTranscription(ctx context.Context, body oai.AudioRequest) (r oai.AudioResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateTranslation(ctx context.Context, body oai.AudioRequest) (r oai.AudioResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateImage(ctx context.Context, body oai.ImageRequest) (r oai.ImageResponse, err error) {
	err = errors.New("unimplemented")
	return
}
