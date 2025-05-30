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

package repository

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Scanner is a mechanism for walking a repository and emitting events for each
// item in the repository.
type Scanner struct {
	fs fs.FS

	onTemplate func(filePath string, contents []byte) error
	onResource func(resourceType, filePath string, contents []byte) error
	onMetrics  func(filePath string, contents []byte) error
	onTracer   func(filePath string, contents []byte) error
}

// NewScanner creates a new scanner with defaults.
func NewScanner(fs fs.FS) *Scanner {
	return &Scanner{
		fs: fs,
	}
}

// OnTemplateFile registers a closure to be called for each template file
// encountered by the scanner.
func (s *Scanner) OnTemplateFile(fn func(filePath string, contents []byte) error) {
	s.onTemplate = fn
}

// OnResourceFile registers a closure to be called for each resource file
// encountered by the scanner.
func (s *Scanner) OnResourceFile(fn func(resourceType, filePath string, contents []byte) error) {
	s.onResource = fn
}

// OnMetricsFile registers a closure to be called for a metrics config file
// encountered by the scanner.
func (s *Scanner) OnMetricsFile(fn func(filePath string, contents []byte) error) {
	s.onMetrics = fn
}

// OnTracerFile registers a closure to be called for a tracer config file
// encountered by the scanner.
func (s *Scanner) OnTracerFile(fn func(filePath string, contents []byte) error) {
	s.onTracer = fn
}

func (s *Scanner) scanFnForExtensions(fn func(path string, contents []byte) error, allowedExtensions ...string) fs.WalkDirFunc {
	allowedExtensionsMap := map[string]struct{}{}
	for _, n := range allowedExtensions {
		allowedExtensionsMap[n] = struct{}{}
	}

	return func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d != nil && d.IsDir() {
			return nil
		}

		if _, exists := allowedExtensionsMap[filepath.Ext(path)]; !exists {
			return nil
		}

		contents, err := fs.ReadFile(s.fs, path)
		if err != nil {
			return fmt.Errorf("%v: %w", path, err)
		}

		if err := fn(path, contents); err != nil {
			return fmt.Errorf("%v: %w", path, err)
		}
		return nil
	}
}

var yamlExtensions = []string{".yml", ".yaml"}

// Scan a target repository at the root provided.
func (s *Scanner) Scan(root string) error {
	if s.onTemplate != nil {
		templatesDir := filepath.Join(root, "templates")

		// All templates are defined in yaml files
		if err := fs.WalkDir(s.fs, templatesDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onTemplate(path, contents)
		}, yamlExtensions...)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if s.onResource != nil {
		// Scan each resource type for files
		resourceDir := filepath.Join(root, "resources")

		// Look for any starlark files in the main resources folder
		if err := fs.WalkDir(s.fs, resourceDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onResource("starlark", path, contents)
		}, "starlark", ".star", ".star.py")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Inputs
		targetDir := filepath.Join(resourceDir, "inputs")
		if err := fs.WalkDir(s.fs, targetDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onResource("input", path, contents)
		}, yamlExtensions...)); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Caches
		targetDir = filepath.Join(resourceDir, "caches")
		if err := fs.WalkDir(s.fs, targetDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onResource("cache", path, contents)
		}, yamlExtensions...)); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Processors
		targetDir = filepath.Join(resourceDir, "processors")
		if err := fs.WalkDir(s.fs, targetDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onResource("processor", path, contents)
		}, yamlExtensions...)); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Outputs
		targetDir = filepath.Join(resourceDir, "outputs")
		if err := fs.WalkDir(s.fs, targetDir, s.scanFnForExtensions(func(path string, contents []byte) error {
			return s.onResource("output", path, contents)
		}, yamlExtensions...)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if s.onMetrics != nil {
		o11yDir := filepath.Join(root, "o11y")
		for _, ext := range yamlExtensions {
			fileName := filepath.Join(o11yDir, "metrics"+ext)
			if contents, err := fs.ReadFile(s.fs, fileName); err == nil {
				if err := s.onMetrics(fileName, contents); err != nil {
					return err
				}
			}
		}
	}

	if s.onTracer != nil {
		o11yDir := filepath.Join(root, "o11y")
		for _, ext := range yamlExtensions {
			fileName := filepath.Join(o11yDir, "tracer"+ext)
			if contents, err := fs.ReadFile(s.fs, fileName); err == nil {
				if err := s.onTracer(fileName, contents); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
