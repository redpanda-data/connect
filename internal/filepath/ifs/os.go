package ifs

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

var _ fs.FS = OS()

// FS is a superset of fs.FS that includes goodies that benthos components
// specifically need.
type FS interface {
	Open(name string) (fs.File, error)
	OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error)
	Stat(name string) (fs.FileInfo, error)
	Remove(name string) error
	MkdirAll(path string, perm fs.FileMode) error
}

// ReadFile opens a file with the RDONLY flag and returns all bytes from it.
func ReadFile(f fs.FS, name string) ([]byte, error) {
	var i fs.File
	var err error
	if ef, ok := f.(FS); ok {
		i, err = ef.OpenFile(name, os.O_RDONLY, 0)
	} else {
		i, err = f.Open(name)
	}
	if err != nil {
		return nil, err
	}
	return io.ReadAll(i)
}

// WriteFile opens a file with O_WRONLY|O_CREATE|O_TRUNC flags and writes the
// data to it.
func WriteFile(f fs.FS, name string, data []byte, perm fs.FileMode) error {
	h, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = h.Write(data)
	if err1 := h.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

// FileWrite attempts to write to an fs.File provided it supports io.Writer.
func FileWrite(file fs.File, data []byte) (int, error) {
	writer, isw := file.(io.Writer)
	if !isw {
		return 0, errors.New("failed to open a writable file")
	}
	return writer.Write(data)
}

// OS implements fs.FS as if calls were being made directly via the os package,
// with which relative paths are resolved from the directory the process is
// executed from.
func OS() FS {
	return osPTI
}

// IsOS returns true if the provided FS implementation is a wrapper around OS
// access obtained via OS().
func IsOS(f FS) bool {
	return f == osPTI
}

var osPTI = &osPT{}

type osPT struct{}

func (o *osPT) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (o *osPT) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (o *osPT) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (o *osPT) Remove(name string) error {
	return os.Remove(name)
}

func (o *osPT) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}
