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

package template

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type opts struct {
	root      string
	renames   map[string]string
	variables map[string]string
}

// Options is a function that modifies the options for the template creation.
type Options func(*opts)

// WithStrippedPrefix allows setting a prefix that will be stripped from the paths in the template filesystem.
func WithStrippedPrefix(prefix string) Options {
	return func(o *opts) {
		o.root = prefix
	}
}

// WithVariables allows setting variables that will be replaced in the template files.
func WithVariables(vars map[string]string) Options {
	return func(o *opts) {
		o.variables = vars
	}
}

// WithRenames allows renaming files during the unpacking process.
func WithRenames(renames map[string]string) Options {
	return func(o *opts) {
		o.renames = renames
	}
}

// CreateTemplate generates the embeded filesystem to the output directory replacing variables found in vars.
func CreateTemplate(tfs fs.ReadFileFS, outputDir string, options ...Options) error {
	o := opts{
		root:      ".",
		renames:   map[string]string{},
		variables: map[string]string{},
	}
	for _, apply := range options {
		apply(&o)
	}
	err := unpackFS(tfs, outputDir, &o)
	if err != nil {
		return fmt.Errorf("failed to generate template: %w", err)
	}
	return nil
}

func unpackFS(tfs fs.ReadFileFS, destPath string, options *opts) error {
	if err := os.MkdirAll(destPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destPath, err)
	}
	oldnew := []string{}
	for k, v := range options.variables {
		oldnew = append(oldnew, k, v)
	}
	replacer := strings.NewReplacer(oldnew...)
	return fs.WalkDir(tfs, options.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk directory %s: %w", path, err)
		}
		relPath, err := filepath.Rel(options.root, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}
		dir, name := filepath.Split(relPath)
		if newName, ok := options.renames[name]; ok {
			name = newName
		}
		outputPath := filepath.Join(destPath, dir, name)
		if d.IsDir() {
			if err := os.MkdirAll(outputPath, os.ModePerm); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", outputPath, err)
			}
			return nil
		}
		data, err := tfs.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}
		f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open file %s for writing: %w", outputPath, err)
		}
		_, err = replacer.WriteString(f, string(data))
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("failed to write file %s: %w", outputPath, err)
		}
		return nil
	})
}
