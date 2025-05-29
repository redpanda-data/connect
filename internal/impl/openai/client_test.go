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

func (*stubClient) CreateEmbeddings(_ context.Context, _ oai.EmbeddingRequestConverter) (r oai.EmbeddingResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateChatCompletion(_ context.Context, _ oai.ChatCompletionRequest) (r oai.ChatCompletionResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateSpeech(_ context.Context, _ oai.CreateSpeechRequest) (r oai.RawResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateTranscription(_ context.Context, _ oai.AudioRequest) (r oai.AudioResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateTranslation(_ context.Context, _ oai.AudioRequest) (r oai.AudioResponse, err error) {
	err = errors.New("unimplemented")
	return
}

func (*stubClient) CreateImage(_ context.Context, _ oai.ImageRequest) (r oai.ImageResponse, err error) {
	err = errors.New("unimplemented")
	return
}
