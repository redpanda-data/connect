// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package text

import (
	"context"
	"errors"
	"sync"
	"testing"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestChunksRecursiveChars(t *testing.T) {
	splits := splitTextUsingConfig(t,
		"Hi, Harrison. \nI am glad to meet you",
		`
text_chunker:
  strategy: recursive_character
  chunk_overlap: 1
  chunk_size: 20
  separators: ["\n", "$"]
`)
	require.Equal(t, []string{"Hi, Harrison.", "I am glad to meet you"}, splits)
}

func TestChunksMarkdown(t *testing.T) {
	markdown := `
## First header: h2
Some content below the first h2.
## Second header: h2
### Third header: h3

- This is a list item of bullet type.
- This is another list item.

 *Everything* is going according to **plan**.

# Fourth header: h1
Some content below the first h1.
## Fifth header: h2
#### Sixth header: h4

Some content below h1>h2>h4.
`
	expected := []string{
		`## First header: h2
Some content below the first h2.`,
		`## Second header: h2`,
		`### Third header: h3
- This is a list item of bullet type.`,
		`### Third header: h3
- This is another list item.`,
		`### Third header: h3
*Everything* is going according to **plan**.`,
		`# Fourth header: h1
Some content below the first h1.`,
		`## Fifth header: h2`,
		`#### Sixth header: h4
Some content below h1>h2>h4.`,
	}
	splits := splitTextUsingConfig(t,
		markdown,
		`
text_chunker:
  strategy: markdown
  chunk_overlap: 64
  chunk_size: 32
`)
	require.Equal(t, expected, splits)
}

func splitTextUsingConfig(t *testing.T, text, config string) []string {
	b := service.NewStreamBuilder()
	producer, err := b.AddBatchProducerFunc()
	require.NoError(t, err)
	var mu sync.Mutex
	var output service.MessageBatch
	err = b.AddBatchConsumerFunc(func(ctx context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		output = append(output, batch...)
		return nil
	})
	require.NoError(t, err)
	err = b.AddProcessorYAML(config)
	require.NoError(t, err)
	s, err := b.Build()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		err = s.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		require.NoError(t, err)
	}()
	err = producer(ctx, service.MessageBatch{service.NewMessage([]byte(text))})
	require.NoError(t, err)
	cancel()
	<-done
	var res []string
	for _, m := range output {
		require.NoError(t, m.GetError())
		b, err := m.AsBytes()
		require.NoError(t, err)
		res = append(res, string(b))
	}
	return res
}
