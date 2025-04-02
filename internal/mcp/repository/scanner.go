/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package repository

import (
	"io/fs"
	"os"
	"path/filepath"
)

// Scanner is a mechanism for walking a repository and emitting events for each
// item in the repository.
type Scanner struct {
	fs fs.FS

	onResource func(resourceType string, filePath string, contents []byte) error
	onMetrics  func(filePath string, contents []byte) error
}

// NewScanner creates a new scanner with defaults.
func NewScanner(fs fs.FS) *Scanner {
	return &Scanner{
		fs: fs,
	}
}

// OnResourceFile registers a closure to be called for each resource file
// encountered by the scanner.
func (s *Scanner) OnResourceFile(fn func(resourceType string, filePath string, contents []byte) error) {
	s.onResource = fn
}

// OnMetricsFile registers a closure to be called for a metrics config file
// encountered by the scanner.
func (s *Scanner) OnMetricsFile(fn func(filePath string, contents []byte) error) {
	s.onMetrics = fn
}

func (s *Scanner) scanResourceTypeFn(rtype string, allowedExtensions ...string) fs.WalkDirFunc {
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
			return err
		}

		return s.onResource(rtype, path, contents)
	}
}

// Scan a target repository at the root provided.
func (s *Scanner) Scan(root string) error {
	if s.onResource != nil {
		// Scan each resource type for files
		resourceDir := filepath.Join(root, "resources")

		// Look for any starlark files in the main resources folder
		if err := fs.WalkDir(s.fs, resourceDir, s.scanResourceTypeFn("starlark", ".star")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Inputs
		targetDir := filepath.Join(resourceDir, "inputs")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("input", ".yaml", ".yml")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Caches
		targetDir = filepath.Join(resourceDir, "caches")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("cache", ".yaml", ".yml")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Processors
		targetDir = filepath.Join(resourceDir, "processors")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("processor", ".yaml", ".yaml")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Outputs
		targetDir = filepath.Join(resourceDir, "outputs")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("output", ".yaml", ".yml")); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	if s.onMetrics != nil {
		o11yDir := filepath.Join(root, "o11y")
		for _, ext := range []string{".yaml", ".yml"} {
			fileName := filepath.Join(o11yDir, "metrics"+ext)
			if contents, err := fs.ReadFile(s.fs, fileName); err == nil {
				if err := s.onMetrics(fileName, contents); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
