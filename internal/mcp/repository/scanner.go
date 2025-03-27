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

func (s *Scanner) scanResourceTypeFn(rtype string) fs.WalkDirFunc {
	return func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d != nil && d.IsDir() {
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

		// Caches
		targetDir := filepath.Join(resourceDir, "caches")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("cache")); err != nil && !os.IsNotExist(err) {
			return err
		}

		// Processors
		targetDir = filepath.Join(resourceDir, "processors")
		if err := fs.WalkDir(s.fs, targetDir, s.scanResourceTypeFn("processor")); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}
