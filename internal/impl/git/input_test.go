// Copyright 2025 Redpanda Data, Inc.
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

package git

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchesPatterns(t *testing.T) {
	t.Run("No Patterns Defined", func(t *testing.T) {
		tests := []struct {
			name    string
			relPath string
			want    bool
		}{
			{
				name:    "README.md accepted",
				relPath: "README.md",
				want:    true,
			},
			{
				name:    "file in subfolder accepted",
				relPath: "docs/manual.md",
				want:    true,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel() // run sub-subtests in parallel
				in := &input{
					cfg: inputCfg{
						includePatterns: nil,
						excludePatterns: nil,
					},
				}
				got := in.matchesPatterns(tt.relPath)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Exclude Patterns Only", func(t *testing.T) {
		tests := []struct {
			name    string
			exclude []string
			relPath string
			want    bool
		}{
			{
				name:    "Exclude single file",
				exclude: []string{"README.md"},
				relPath: "README.md",
				want:    false,
			},
			{
				name:    "Exclude all markdown files",
				exclude: []string{"**/*.md"},
				relPath: "docs/manual.md",
				want:    false,
			},
			{
				name:    "Exclude docs folder, .md outside is okay",
				exclude: []string{"docs/*"},
				relPath: "some_folder/readme.md",
				want:    true,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := &input{
					cfg: inputCfg{
						includePatterns: nil,
						excludePatterns: tt.exclude,
					},
				}
				got := in.matchesPatterns(tt.relPath)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Include Patterns Only", func(t *testing.T) {
		tests := []struct {
			name    string
			include []string
			relPath string
			want    bool
		}{
			{
				name:    "Include only .md files",
				include: []string{"**/*.md"},
				relPath: "manual.md",
				want:    true,
			},
			{
				name:    "Include only .md files, any subdirectory",
				include: []string{"**/*.md"},
				relPath: "docs/manual.md",
				want:    true,
			},
			{
				name:    "Include only .go files, non-matching .md fails",
				include: []string{"*.go"},
				relPath: "docs/manual.md",
				want:    false,
			},
			{
				name:    "Include any file from subdirectory",
				include: []string{"docs/**"},
				relPath: "docs/nested/getting-started.md",
				want:    true,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := &input{
					cfg: inputCfg{
						includePatterns: tt.include,
						excludePatterns: nil,
					},
				}
				got := in.matchesPatterns(tt.relPath)
				assert.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Mixed Include/Exclude Patterns", func(t *testing.T) {
		tests := []struct {
			name    string
			include []string
			exclude []string
			relPath string
			want    bool
		}{
			{
				name:    "Include *.go, exclude *_test.go",
				include: []string{"*.go"},
				exclude: []string{"*_test.go"},
				relPath: "example_test.go",
				want:    false,
			},
			{
				name:    "Include *.go, exclude *_test.go (main.go included)",
				include: []string{"*.go"},
				exclude: []string{"*_test.go"},
				relPath: "main.go",
				want:    true,
			},
			{
				name:    "Multiple includes, single exclude",
				include: []string{"*.go", "*.md"},
				exclude: []string{"CHANGELOG.md"},
				relPath: "CHANGELOG.md",
				want:    false,
			},
			{
				name:    "Multiple includes, single exclude (docs/readme.md included)",
				include: []string{"**/*.go", "**/*.md"},
				exclude: []string{"CHANGELOG.md"},
				relPath: "docs/readme.md",
				want:    true,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				in := &input{
					cfg: inputCfg{
						includePatterns: tt.include,
						excludePatterns: tt.exclude,
					},
				}
				got := in.matchesPatterns(tt.relPath)
				assert.Equal(t, tt.want, got)
			})
		}
	})
}

func TestDetectMimeType(t *testing.T) {
	in := &input{log: service.MockResources().Logger()}

	tmpDir := t.TempDir()

	// Helper to create a temp file with content
	createTempFile := func(t *testing.T, fileName string, content []byte) string {
		filePath := filepath.Join(tmpDir, fileName)

		tmpFile, err := os.Create(filePath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			tmpFile.Close()
		})

		_, err = tmpFile.Write(content)
		require.NoError(t, err)

		tmpFile.Close()
		return tmpFile.Name()
	}

	tests := []struct {
		name         string
		fileName     string
		content      []byte
		wantMime     string
		wantIsBinary bool
	}{
		{
			name:         "Empty file with .txt",
			fileName:     "empty.txt",
			content:      []byte(""),
			wantMime:     "text/plain",
			wantIsBinary: false,
		},
		{
			name:         "Simple text file .log",
			fileName:     "example.log",
			content:      []byte("This is a log file"),
			wantMime:     "text/plain",
			wantIsBinary: false,
		},
		{
			name:         "Markdown file .md",
			fileName:     "readme-*.md",
			content:      []byte("# Markdown heading"),
			wantMime:     "text/markdown",
			wantIsBinary: false,
		},
		{
			name:         "CSV file .csv",
			fileName:     "data-*.csv",
			content:      []byte("col1,col2\nval1,val2"),
			wantMime:     "text/csv",
			wantIsBinary: false,
		},
		{
			name:         "JSON file .json",
			fileName:     "data-*.json",
			content:      []byte(`{"key":"value"}`),
			wantMime:     "application/json",
			wantIsBinary: false,
		},
		{
			name:         "PNG file .png with signature",
			fileName:     "image-*.png",
			content:      []byte{0x89, 0x50, 0x4E, 0x47},
			wantMime:     "image/png",
			wantIsBinary: true,
		},
		{
			name:         "JPEG file .jpg with signature",
			fileName:     "photo.jpg",
			content:      []byte{0xFF, 0xD8, 0xFF},
			wantMime:     "image/jpeg",
			wantIsBinary: true,
		},
		{
			name:         "BIN file .bin",
			fileName:     "data.bin",
			content:      []byte{0x00, 0x01, 0xFF},
			wantMime:     "application/octet-stream",
			wantIsBinary: true,
		},
		{
			name:         "Python script .py",
			fileName:     "script.py",
			content:      []byte("#!/usr/bin/env python\nprint('Hello')"),
			wantMime:     "text/plain",
			wantIsBinary: false,
		},
		{
			name:     "Unknown extension but text content",
			fileName: "unknown.xyz",
			content:  []byte("This is likely text"),
			// In this case, extensionToMIME lookup fails,
			// so we rely on content detection -> http.DetectContentType => "text/plain"
			wantMime:     "text/plain",
			wantIsBinary: false,
		},
		{
			name:     "No extension, text content",
			fileName: "filewithoutext",
			content:  []byte("No extension, pure text"),
			// Content detection => "text/plain"
			wantMime:     "text/plain",
			wantIsBinary: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpFilePath := createTempFile(t, tc.fileName, tc.content)
			defer os.Remove(tmpFilePath)

			gotMime, gotIsBinary := in.detectMimeType(tmpFilePath)
			assert.Equal(t, tc.wantMime, gotMime, "MIME type mismatch")
			assert.Equal(t, tc.wantIsBinary, gotIsBinary, "Binary status mismatch")
		})
	}
}
