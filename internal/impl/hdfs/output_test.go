// Copyright 2026 Redpanda Data, Inc.
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

package hdfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type fakeFileWriter struct {
	buf      bytes.Buffer
	writeErr error
	closeErr error
	closed   bool
}

func (f *fakeFileWriter) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.buf.Write(p)
}

func (f *fakeFileWriter) Close() error {
	f.closed = true
	return f.closeErr
}

type fakeHDFSClient struct {
	writers map[string]*fakeFileWriter
	newFile func() *fakeFileWriter
}

func (*fakeHDFSClient) MkdirAll(string, os.FileMode) error {
	return nil
}

func (c *fakeHDFSClient) Create(name string) (io.WriteCloser, error) {
	fw := c.newFile()
	c.writers[name] = fw
	return fw, nil
}

func newTestHDFSWriter(t *testing.T, newFile func() *fakeFileWriter) (*hdfsWriter, *fakeHDFSClient) {
	t.Helper()

	directory, err := service.NewInterpolatedString("/out")
	require.NoError(t, err)
	path, err := service.NewInterpolatedString("${! content() }.txt")
	require.NoError(t, err)

	client := &fakeHDFSClient{
		writers: map[string]*fakeFileWriter{},
		newFile: newFile,
	}
	return &hdfsWriter{
		directory: directory,
		path:      path,
		client:    client,
	}, client
}

func TestHDFSOutputWriteBatch(t *testing.T) {
	w, client := newTestHDFSWriter(t, func() *fakeFileWriter { return &fakeFileWriter{} })

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte("foo")),
		service.NewMessage([]byte("bar")),
	})
	require.NoError(t, err)

	require.Len(t, client.writers, 2)
	for _, name := range []string{"foo", "bar"} {
		fw := client.writers["/out/"+name+".txt"]
		require.NotNil(t, fw)
		assert.Equal(t, name, fw.buf.String())
		assert.True(t, fw.closed)
	}
}

func TestHDFSOutputWriteErrorClosesFile(t *testing.T) {
	writeErr := errors.New("write failed")
	w, client := newTestHDFSWriter(t, func() *fakeFileWriter {
		return &fakeFileWriter{writeErr: writeErr}
	})

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte("foo")),
	})
	require.ErrorIs(t, err, writeErr)

	fw := client.writers["/out/foo.txt"]
	require.NotNil(t, fw)
	assert.True(t, fw.closed)
}

func TestHDFSOutputCloseErrorIsReturned(t *testing.T) {
	closeErr := errors.New("file is not complete")
	w, client := newTestHDFSWriter(t, func() *fakeFileWriter {
		return &fakeFileWriter{closeErr: closeErr}
	})

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte("foo")),
	})
	require.ErrorIs(t, err, closeErr)

	fw := client.writers["/out/foo.txt"]
	require.NotNil(t, fw)
	assert.True(t, fw.closed)
}
