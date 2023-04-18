package studio

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
)

// Implements ifs.FS around the Benthos Studio node APIs.
type sessionFS struct {
	tracker *sessionTracker
}

func (s *sessionFS) Open(name string) (fs.File, error) {
	return s.tracker.ReadFile(context.Background(), name)
}

func (s *sessionFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return s.Open(name)
}

func (s *sessionFS) Stat(name string) (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func (s *sessionFS) Remove(name string) error {
	return errors.New("not implemented")
}

func (s *sessionFS) MkdirAll(path string, perm fs.FileMode) error {
	return errors.New("not implemented")
}

//------------------------------------------------------------------------------

type sessionFile struct {
	res *http.Response
}

func (s *sessionFile) Stat() (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func (s *sessionFile) Read(b []byte) (int, error) {
	return s.res.Body.Read(b)
}

func (s *sessionFile) Close() error {
	return s.res.Body.Close()
}
