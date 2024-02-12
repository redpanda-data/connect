package ifs

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
)

var _ http.FileSystem = ToHTTP(OS())

type asHTTP struct {
	f fs.FS
}

// ToHTTP converts an fs.FS into an http.FileSystem in a way that doesn't
// modify the root path.
func ToHTTP(f fs.FS) *asHTTP {
	return &asHTTP{f: f}
}

func (h *asHTTP) Open(name string) (http.File, error) {
	f, err := h.f.Open(name)
	if err != nil {
		return nil, err
	}
	return asHTTPFile{file: f}, nil
}

func (f asHTTPFile) ReadDir(count int) ([]fs.DirEntry, error) {
	d, ok := f.file.(fs.ReadDirFile)
	if !ok {
		return nil, errMissingReadDir
	}
	return d.ReadDir(count)
}

type asHTTPFile struct {
	file fs.File
}

func (f asHTTPFile) Close() error               { return f.file.Close() }
func (f asHTTPFile) Read(b []byte) (int, error) { return f.file.Read(b) }
func (f asHTTPFile) Stat() (fs.FileInfo, error) { return f.file.Stat() }

var errMissingSeek = errors.New("io.File missing Seek method")
var errMissingReadDir = errors.New("io.File directory missing ReadDir method")

func (f asHTTPFile) Seek(offset int64, whence int) (int64, error) {
	s, ok := f.file.(io.Seeker)
	if !ok {
		return 0, errMissingSeek
	}
	return s.Seek(offset, whence)
}

func (f asHTTPFile) Readdir(count int) ([]fs.FileInfo, error) {
	d, ok := f.file.(fs.ReadDirFile)
	if !ok {
		return nil, errMissingReadDir
	}
	var list []fs.FileInfo
	for {
		dirs, err := d.ReadDir(count - len(list))
		for _, dir := range dirs {
			info, err := dir.Info()
			if err != nil {
				// Pretend it doesn't exist, like (*os.File).Readdir does.
				continue
			}
			list = append(list, info)
		}
		if err != nil {
			return list, err
		}
		if count < 0 || len(list) >= count {
			break
		}
	}
	return list, nil
}
