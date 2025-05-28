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
	"fmt"
	"unicode/utf8"

	"github.com/pkoukk/tiktoken-go"
	"github.com/rivo/uniseg"
	"github.com/tmc/langchaingo/textsplitter"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ service.Processor = (*textChunker)(nil)

func init() {
	service.MustRegisterProcessor(
		"text_chunker",
		newTextChunkerSpec(),
		newTextChunker,
	)
}

const (
	tcpFieldStrategy          = "strategy"
	tcpFieldChunkSize         = "chunk_size"
	tcpFieldChunkOverlap      = "chunk_overlap"
	tcpFieldSeparators        = "separators"
	tcpFieldWithLenFunc       = "length_measure"
	tcpFieldTokenEncoding     = "token_encoding"
	tcpFieldAllowedSpecial    = "allowed_special"
	tcpFieldDisallowedSpecial = "disallowed_special"
	tcpFieldIncludeCodeBlocks = "include_code_blocks"
	tcpFieldReferenceLinks    = "keep_reference_links"
)

func newTextChunkerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("A processor that allows chunking and splitting text based on some strategy. Usually used for creating vector embeddings of large documents.").
		Description(`A processor allowing splitting text into chunks based on several different strategies.`).
		Fields(
			service.NewStringAnnotatedEnumField(tcpFieldStrategy, map[string]string{
				"recursive_character": "Split text recursively by characters (defined in `separators`).",
				"markdown":            "Split text by markdown headers.",
				"token":               "Split text by tokens.",
			}),
			service.NewIntField(tcpFieldChunkSize).
				Description("The maximum size of each chunk.").
				Default(textsplitter.DefaultOptions().ChunkSize),
			service.NewIntField(tcpFieldChunkOverlap).
				Description("The number of characters to overlap between chunks.").
				Default(textsplitter.DefaultOptions().ChunkOverlap),
			service.NewStringListField(tcpFieldSeparators).
				Description("A list of strings that should be considered as separators between chunks.").
				Default(textsplitter.DefaultOptions().Separators),
			service.NewStringAnnotatedEnumField(tcpFieldWithLenFunc, map[string]string{
				"utf8":      "Determine the length of text using the number of utf8 bytes.",
				"runes":     "Use the number of codepoints to determine the length of a string.",
				"token":     "Use the number of tokens (using the `token_encoding` tokenizer) to determine the length of a string.",
				"graphemes": "Use unicode graphemes to determine the length of a string.",
			}).
				Description("The method for measuring the length of a string.").
				Default("runes"),
			service.NewStringField(tcpFieldTokenEncoding).
				Optional().
				Advanced().
				Description("The encoding to use for tokenization.").
				Example("cl100k_base").
				Example("r50k_base"),
			service.NewStringListField(tcpFieldAllowedSpecial).
				Advanced().
				Default(textsplitter.DefaultOptions().AllowedSpecial).
				Description("A list of special tokens that are allowed in the output."),
			service.NewStringListField(tcpFieldDisallowedSpecial).
				Advanced().
				Default(textsplitter.DefaultOptions().DisallowedSpecial).
				Description("A list of special tokens that are disallowed in the output."),
			service.NewBoolField(tcpFieldIncludeCodeBlocks).
				Default(textsplitter.DefaultOptions().CodeBlocks).
				Description("Whether to include code blocks in the output."),
			service.NewBoolField(tcpFieldReferenceLinks).
				Default(textsplitter.DefaultOptions().ReferenceLinks).
				Description("Whether to keep reference links in the output."),
		)
}

func newTextChunker(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	processor := &textChunker{}
	opts := []textsplitter.Option{}

	chunkSize, err := conf.FieldInt(tcpFieldChunkSize)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithChunkSize(chunkSize))

	chunkOverlap, err := conf.FieldInt(tcpFieldChunkOverlap)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithChunkOverlap(chunkOverlap))

	seps, err := conf.FieldStringList(tcpFieldSeparators)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithSeparators(seps))

	referenceLinks, err := conf.FieldBool(tcpFieldReferenceLinks)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithReferenceLinks(referenceLinks))

	codeBlocks, err := conf.FieldBool(tcpFieldIncludeCodeBlocks)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithCodeBlocks(codeBlocks))

	var tokenizer *tiktoken.Tiktoken
	if conf.Contains(tcpFieldTokenEncoding) {
		encoding, err := conf.FieldString(tcpFieldTokenEncoding)
		if err != nil {
			return nil, err
		}
		tokenizer, err = tiktoken.GetEncoding(encoding)
		if err != nil {
			return nil, fmt.Errorf("failed to get tokenizer for encoding '%v': %w", encoding, err)
		}
		opts = append(opts, textsplitter.WithEncodingName(encoding))
	}

	allowedSpecial, err := conf.FieldStringList(tcpFieldAllowedSpecial)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithAllowedSpecial(allowedSpecial))

	disallowedSpecial, err := conf.FieldStringList(tcpFieldDisallowedSpecial)
	if err != nil {
		return nil, err
	}
	opts = append(opts, textsplitter.WithDisallowedSpecial(disallowedSpecial))

	lenFuncStr, err := conf.FieldString(tcpFieldWithLenFunc)
	if err != nil {
		return nil, err
	}
	switch lenFuncStr {
	case "utf8":
		opts = append(opts, textsplitter.WithLenFunc(func(s string) int { return len(s) }))
	case "runes":
		opts = append(opts, textsplitter.WithLenFunc(utf8.RuneCountInString))
	case "token":
		if tokenizer == nil {
			return nil, fmt.Errorf("token length measure requires %s", tcpFieldTokenEncoding)
		}
		opts = append(opts, textsplitter.WithLenFunc(func(s string) int {
			return len(tokenizer.Encode(s, allowedSpecial, disallowedSpecial))
		}))
	case "graphemes":
		opts = append(opts, textsplitter.WithLenFunc(func(s string) int { return uniseg.GraphemeClusterCount(s) }))
	default:
		return nil, fmt.Errorf("unknown %s: %v", tcpFieldWithLenFunc, lenFuncStr)
	}

	strat, err := conf.FieldString(tcpFieldStrategy)
	if err != nil {
		return nil, err
	}
	switch strat {
	case "recursive_character":
		s := textsplitter.NewRecursiveCharacter(opts...)
		processor.splitter = s
	case "markdown":
		processor.splitter = textsplitter.NewMarkdownTextSplitter(opts...)
	case "token":
		processor.splitter = textsplitter.NewTokenSplitter(opts...)
	default:
		return nil, fmt.Errorf("unknown %s: %v", tcpFieldStrategy, strat)
	}
	return processor, nil
}

type textChunker struct {
	splitter textsplitter.TextSplitter
}

// Process implements service.Processor.
func (t *textChunker) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	b, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	texts, err := t.splitter.SplitText(string(b))
	if err != nil {
		return nil, err
	}
	batch := make(service.MessageBatch, len(texts))
	for i, text := range texts {
		cpy := msg.Copy()
		cpy.SetBytes([]byte(text))
		batch[i] = cpy
	}
	return batch, nil
}

// Close implements service.Processor.
func (t *textChunker) Close(ctx context.Context) error {
	return nil
}
