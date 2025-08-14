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
