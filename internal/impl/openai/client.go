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
