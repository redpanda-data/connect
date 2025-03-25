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
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/text/chunker"
	"github.com/rivo/uniseg"
)

var _ service.Processor = (*textChunker)(nil)

func init() {
	err := service.RegisterProcessor(
		"text_chunker",
		newTextChunkerSpec(),
		newTextChunker,
	)
	if err != nil {
		panic(err)
	}
}

const (
	tcpFieldStrategy             = "strategy"
	tcpFieldChunkSize            = "chunk_size"
	tcpFieldChunkOverlap         = "chunk_overlap"
	tcpFieldSeparators           = "separators"
	tcpFieldWithLenFunc          = "length_measure"
	tcpFieldTokenEncoding        = "token_encoding"
	tcpFieldAllowedSpecial       = "allowed_special"
	tcpFieldDisallowedSpecial    = "disallowed_special"
	tcpFieldIncludeCodeBlocks    = "include_code_blocks"
	tcpFieldSecondarySplitter    = "secondary_splitter"
	tcpFieldReferenceLinks       = "keep_reference_links"
	tcpFieldKeepSeparator        = "keep_separator"
	tcpFieldKeepHeadingHierarchy = "keep_heading_hierarchy"
	tcpFieldJoinTableRows        = "join_table_rows"
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
				Default(chunker.DefaultOptions().ChunkSize),
			service.NewIntField(tcpFieldChunkOverlap).
				Description("The number of characters to overlap between chunks.").
				Default(chunker.DefaultOptions().ChunkOverlap),
			service.NewStringListField(tcpFieldSeparators).
				Description("A list of strings that should be considered as separators between chunks.").
				Default(chunker.DefaultOptions().Separators),
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
				Default(chunker.DefaultOptions().AllowedSpecial).
				Description("A list of special tokens that are allowed in the output."),
			service.NewStringListField(tcpFieldDisallowedSpecial).
				Advanced().
				Default(chunker.DefaultOptions().DisallowedSpecial).
				Description("A list of special tokens that are disallowed in the output."),
			service.NewBoolField(tcpFieldIncludeCodeBlocks).
				Default(chunker.DefaultOptions().CodeBlocks).
				Description("Whether to include code blocks in the output."),
			service.NewProcessorField(tcpFieldSecondarySplitter).
				Optional().
				Advanced().
				Example(map[string]any{
					"text_chunker": map[string]any{
						tcpFieldStrategy: "token",
					},
				}).
				Description("A secondary text splitter to apply to each chunk after the initial split."),
			service.NewBoolField(tcpFieldReferenceLinks).
				Default(chunker.DefaultOptions().ReferenceLinks).
				Description("Whether to keep reference links in the output."),
			service.NewBoolField(tcpFieldKeepSeparator).Default(chunker.DefaultOptions().KeepSeparator).Description("Whether to keep the separator in the output. When set to true the seperators are included in the resulting split text."),
			service.NewBoolField(tcpFieldKeepHeadingHierarchy).Default(chunker.DefaultOptions().KeepHeadingHierarchy).Description("Whether to keep the hierarchy of markdown headers in each chunk. When true each chunk gets prepended with a list of all parent headings in the hierarchy up to this point."),
			service.NewBoolField(tcpFieldJoinTableRows).Default(chunker.DefaultOptions().JoinTableRows).Description("Whether to join markdown table rows in the output."),
		)

}

func newTextChunker(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	processor := &textChunker{}
	opts := []chunker.Option{}

	chunkSize, err := conf.FieldInt(tcpFieldChunkSize)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithChunkSize(chunkSize))

	chunkOverlap, err := conf.FieldInt(tcpFieldChunkOverlap)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithChunkOverlap(chunkOverlap))

	seps, err := conf.FieldStringList(tcpFieldSeparators)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithSeparators(seps))

	referenceLinks, err := conf.FieldBool(tcpFieldReferenceLinks)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithReferenceLinks(referenceLinks))

	keepSeparator, err := conf.FieldBool(tcpFieldKeepSeparator)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithKeepSeparator(keepSeparator))

	keepHeadingHierarchy, err := conf.FieldBool(tcpFieldKeepHeadingHierarchy)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithHeadingHierarchy(keepHeadingHierarchy))

	joinTableRows, err := conf.FieldBool(tcpFieldJoinTableRows)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithJoinTableRows(joinTableRows))

	if conf.Contains(tcpFieldSecondarySplitter) {
		secondaryProcessor, err := conf.FieldProcessor(tcpFieldSecondarySplitter)
		if err != nil {
			return nil, err
		}
		processor.secondary = &processorSplitter{context: nil, processor: secondaryProcessor}
		opts = append(opts, chunker.WithSecondSplitter(processor.secondary))
	}

	codeBlocks, err := conf.FieldBool(tcpFieldIncludeCodeBlocks)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithCodeBlocks(codeBlocks))

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
		opts = append(opts, chunker.WithEncodingName(encoding))
	}

	allowedSpecial, err := conf.FieldStringList(tcpFieldAllowedSpecial)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithAllowedSpecial(allowedSpecial))

	disallowedSpecial, err := conf.FieldStringList(tcpFieldDisallowedSpecial)
	if err != nil {
		return nil, err
	}
	opts = append(opts, chunker.WithDisallowedSpecial(disallowedSpecial))

	lenFuncStr, err := conf.FieldString(tcpFieldWithLenFunc)
	if err != nil {
		return nil, err
	}
	switch lenFuncStr {
	case "utf8":
		opts = append(opts, chunker.WithLenFunc(func(s string) int { return len(s) }))
	case "runes":
		opts = append(opts, chunker.WithLenFunc(utf8.RuneCountInString))
	case "token":
		if tokenizer == nil {
			return nil, fmt.Errorf("token length measure requires %s", tcpFieldTokenEncoding)
		}
		opts = append(opts, chunker.WithLenFunc(func(s string) int {
			return len(tokenizer.Encode(s, allowedSpecial, disallowedSpecial))
		}))
	case "graphemes":
		opts = append(opts, chunker.WithLenFunc(func(s string) int { return uniseg.GraphemeClusterCount(s) }))
	default:
		return nil, fmt.Errorf("unknown %s: %v", tcpFieldWithLenFunc, lenFuncStr)
	}

	strat, err := conf.FieldString(tcpFieldStrategy)
	if err != nil {
		return nil, err
	}
	switch strat {
	case "recursive_character":
		s := chunker.NewRecursiveCharacter(opts...)
		processor.splitter = s
	case "markdown":
		processor.splitter = chunker.NewMarkdownTextSplitter(opts...)
	case "token":
		processor.splitter = chunker.NewTokenSplitter(opts...)
	default:
		return nil, fmt.Errorf("unknown %s: %v", tcpFieldStrategy, strat)
	}
	return processor, nil
}

type textChunker struct {
	splitter  chunker.TextSplitter
	secondary *processorSplitter
}

// Process implements service.Processor.
func (t *textChunker) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	if t.secondary != nil {
		t.secondary.context = ctx
		defer func() { t.secondary.context = nil }()
	}
	return chunker.SplitMessage(t.splitter, msg)
}

// Close implements service.Processor.
func (t *textChunker) Close(ctx context.Context) error {
	if t.secondary != nil {
		if err := t.secondary.processor.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

type processorSplitter struct {
	context   context.Context
	processor *service.OwnedProcessor
}

// SplitText implements chunker.TextSplitter.
func (p *processorSplitter) SplitText(text string) ([]string, error) {
	batch, err := p.processor.Process(p.context, service.NewMessage([]byte(text)))
	if err != nil {
		return nil, err
	}
	var output []string
	err = batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}
		output = append(output, string(b))
		return nil
	})
	return output, err
}
