// The MIT License
//
// Copyright (c) Travis Cline <travis.cline@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package chunker

import (
	"fmt"

	"github.com/pkoukk/tiktoken-go"
)

const (
	// nolint:gosec
	_defaultTokenModelName    = "gpt-3.5-turbo"
	_defaultTokenEncoding     = "cl100k_base"
	_defaultTokenChunkSize    = 512
	_defaultTokenChunkOverlap = 100
)

// TokenSplitter is a text splitter that will split texts by tokens.
type TokenSplitter struct {
	ChunkSize         int
	ChunkOverlap      int
	ModelName         string
	EncodingName      string
	AllowedSpecial    []string
	DisallowedSpecial []string
}

func NewTokenSplitter(opts ...Option) TokenSplitter {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	s := TokenSplitter{
		ChunkSize:         options.ChunkSize,
		ChunkOverlap:      options.ChunkOverlap,
		ModelName:         options.ModelName,
		EncodingName:      options.EncodingName,
		AllowedSpecial:    options.AllowedSpecial,
		DisallowedSpecial: options.DisallowedSpecial,
	}

	return s
}

// SplitText splits a text into multiple text.
func (s TokenSplitter) SplitText(text string) ([]string, error) {
	// Get the tokenizer
	var tk *tiktoken.Tiktoken
	var err error
	if s.EncodingName != "" {
		tk, err = tiktoken.GetEncoding(s.EncodingName)
	} else {
		tk, err = tiktoken.EncodingForModel(s.ModelName)
	}
	if err != nil {
		return nil, fmt.Errorf("tiktoken.GetEncoding: %w", err)
	}
	texts := s.splitText(text, tk)

	return texts, nil
}

func (s TokenSplitter) splitText(text string, tk *tiktoken.Tiktoken) []string {
	splits := make([]string, 0)
	inputIDs := tk.Encode(text, s.AllowedSpecial, s.DisallowedSpecial)

	startIdx := 0
	curIdx := len(inputIDs)
	if startIdx+s.ChunkSize < curIdx {
		curIdx = startIdx + s.ChunkSize
	}
	for startIdx < len(inputIDs) {
		chunkIDs := inputIDs[startIdx:curIdx]
		splits = append(splits, tk.Decode(chunkIDs))
		startIdx += s.ChunkSize - s.ChunkOverlap
		curIdx = startIdx + s.ChunkSize
		if curIdx > len(inputIDs) {
			curIdx = len(inputIDs)
		}
	}
	return splits
}
