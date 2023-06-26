package studio

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

// Implements ifs.FS around the Benthos Studio node APIs.
type sessionFS struct {
	tracker *sessionTracker
	backup  ifs.FS
}

func (s *sessionFS) Open(name string) (fs.File, error) {
	f, err := s.tracker.ReadFile(context.Background(), name, false)
	if err != nil && errors.Is(err, fs.ErrNotExist) && s.backup != nil {
		f, err = s.backup.Open(name)
	}
	return f, err
}

func (s *sessionFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	if flag == os.O_RDONLY {
		return s.Open(name)
	}
	if s.backup == nil {
		return nil, errors.New("not implemented")
	}
	return s.backup.OpenFile(name, flag, perm)
}

func (s *sessionFS) Stat(name string) (fs.FileInfo, error) {
	f, err := s.tracker.ReadFile(context.Background(), name, true)
	if err != nil && errors.Is(err, fs.ErrNotExist) && s.backup != nil {
		return s.backup.Stat(name)
	}
	if err != nil {
		return nil, err
	}
	return f.Stat()
}

func (s *sessionFS) Remove(name string) error {
	if s.backup == nil {
		return errors.New("not implemented")
	}
	return s.backup.Remove(name)
}

func (s *sessionFS) MkdirAll(path string, perm fs.FileMode) error {
	if s.backup == nil {
		return errors.New("not implemented")
	}
	return s.backup.MkdirAll(path, perm)
}

//------------------------------------------------------------------------------

type sessionFile struct {
	res     *http.Response
	path    string
	modTime time.Time
}

func (s *sessionFile) Stat() (fs.FileInfo, error) {
	return &sessionFileInfo{
		path:    s.path,
		size:    s.res.ContentLength,
		modTime: s.modTime,
	}, nil
}

func (s *sessionFile) Read(b []byte) (int, error) {
	return s.res.Body.Read(b)
}

func (s *sessionFile) Close() error {
	return s.res.Body.Close()
}

//------------------------------------------------------------------------------

type sessionFileInfo struct {
	path    string
	size    int64
	modTime time.Time
}

func (s *sessionFileInfo) Name() string {
	return filepath.Base(s.path)
}

func (s *sessionFileInfo) Size() int64 {
	return s.size
}

func (s *sessionFileInfo) Mode() fs.FileMode {
	return fs.ModePerm
}

func (s *sessionFileInfo) ModTime() time.Time {
	return s.modTime
}

func (s *sessionFileInfo) IsDir() bool {
	return false
}

func (s *sessionFileInfo) Sys() any {
	return nil
}
