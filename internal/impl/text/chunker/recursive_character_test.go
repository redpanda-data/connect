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
//
// Forked from https://github.com/tmc/langchaingo/blob/main/LICENSE

package chunker

import (
	"strings"
	"testing"

	"github.com/pkoukk/tiktoken-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecursiveCharacterSplitter(t *testing.T) {
	tokenEncoder, _ := tiktoken.GetEncoding("cl100k_base")

	t.Parallel()
	type testCase struct {
		text          string
		chunkOverlap  int
		chunkSize     int
		separators    []string
		expectedDocs  []Document
		keepSeparator bool
		LenFunc       func(string) int
	}
	testCases := []testCase{
		{
			text:         "哈里森\n很高兴遇见你\n欢迎来中国",
			chunkOverlap: 0,
			chunkSize:    10,
			separators:   []string{"\n\n", "\n", " "},
			expectedDocs: []Document{
				{PageContent: "哈里森\n很高兴遇见你"},
				{PageContent: "欢迎来中国"},
			},
		},
		{
			text:         "Hi, Harrison. \nI am glad to meet you",
			chunkOverlap: 1,
			chunkSize:    20,
			separators:   []string{"\n", "$"},
			expectedDocs: []Document{
				{PageContent: "Hi, Harrison."},
				{PageContent: "I am glad to meet you"},
			},
		},
		{
			text:         "Hi.\nI'm Harrison.\n\nHow?\na\nbHi.\nI'm Harrison.\n\nHow?\na\nb",
			chunkOverlap: 1,
			chunkSize:    40,
			separators:   []string{"\n\n", "\n", " ", ""},
			expectedDocs: []Document{
				{PageContent: "Hi.\nI'm Harrison."},
				{PageContent: "How?\na\nbHi.\nI'm Harrison.\n\nHow?\na\nb"},
			},
		},
		{
			text:         "name: Harrison\nage: 30",
			chunkOverlap: 1,
			chunkSize:    40,
			separators:   []string{"\n\n", "\n", " ", ""},
			expectedDocs: []Document{
				{PageContent: "name: Harrison\nage: 30"},
			},
		},
		{
			text: `name: Harrison
age: 30

name: Joe
age: 32`,
			chunkOverlap: 1,
			chunkSize:    40,
			separators:   []string{"\n\n", "\n", " ", ""},
			expectedDocs: []Document{
				{PageContent: "name: Harrison\nage: 30"},
				{PageContent: "name: Joe\nage: 32"},
			},
		},
		{
			text: `Hi.
I'm Harrison.

How? Are? You?
Okay then f f f f.
This is a weird text to write, but gotta test the splittingggg some how.

Bye!

-H.`,
			chunkOverlap: 1,
			chunkSize:    10,
			separators:   []string{"\n\n", "\n", " ", ""},
			expectedDocs: []Document{
				{PageContent: "Hi."},
				{PageContent: "I'm"},
				{PageContent: "Harrison."},
				{PageContent: "How? Are?"},
				{PageContent: "You?"},
				{PageContent: "Okay then"},
				{PageContent: "f f f f."},
				{PageContent: "This is a"},
				{PageContent: "a weird"},
				{PageContent: "text to"},
				{PageContent: "write, but"},
				{PageContent: "gotta test"},
				{PageContent: "the"},
				{PageContent: "splittingg"},
				{PageContent: "ggg"},
				{PageContent: "some how."},
				{PageContent: "Bye!\n\n-H."},
			},
		},
		{
			text:          "Hi, Harrison. \nI am glad to meet you",
			chunkOverlap:  0,
			chunkSize:     10,
			separators:    []string{"\n", "$"},
			keepSeparator: true,
			expectedDocs: []Document{
				{PageContent: "Hi, Harrison. "},
				{PageContent: "\nI am glad to meet you"},
			},
		},
		{
			text:          strings.Repeat("The quick brown fox jumped over the lazy dog. ", 2),
			chunkOverlap:  0,
			chunkSize:     10,
			separators:    []string{" "},
			keepSeparator: true,
			LenFunc:       func(s string) int { return len(tokenEncoder.Encode(s, nil, nil)) },
			expectedDocs: []Document{
				{PageContent: "The quick brown fox jumped over the lazy dog."},
				{PageContent: "The quick brown fox jumped over the lazy dog."},
			},
		},
	}
	splitter := NewRecursiveCharacter()
	for _, tc := range testCases {
		splitter.ChunkOverlap = tc.chunkOverlap
		splitter.ChunkSize = tc.chunkSize
		splitter.Separators = tc.separators
		splitter.KeepSeparator = tc.keepSeparator
		if tc.LenFunc != nil {
			splitter.LenFunc = tc.LenFunc
		}

		docs, err := CreateDocuments(splitter, []string{tc.text}, nil)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}
