package pure

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestArchiveBadAlgo(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: does not exist
`, nil)
	require.NoError(t, err)

	_, err = newArchiveFromParsed(conf, service.MockResources())
	assert.EqualError(t, err, "archive format not recognised: does not exist")
}

func TestArchiveTar(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: tar
path: 'foo-${!meta("path")}'
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch
	for i, e := range exp {
		p := service.NewMessage(e)
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		msg = append(msg, p)
	}

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	bBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	buf := bytes.NewBuffer(bBytes)
	tr := tar.NewReader(buf)
	i := 0
	for {
		var hdr *tar.Header
		hdr, err = tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		require.NoError(t, err)

		newPartBuf := bytes.Buffer{}
		_, err = newPartBuf.ReadFrom(tr)
		require.NoError(t, err)

		assert.Equal(t, string(exp[i]), newPartBuf.String())
		assert.Equal(t, fmt.Sprintf("foo-bar%v", i), hdr.FileInfo().Name())
		i++
	}

	assert.Equal(t, len(exp), i)
}

func TestArchiveZip(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: zip
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch
	for i, e := range exp {
		p := service.NewMessage(e)
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		msg = append(msg, p)
	}

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	bBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	buf := bytes.NewReader(bBytes)
	zr, err := zip.NewReader(buf, int64(buf.Len()))
	require.NoError(t, err)

	i := 0
	for _, f := range zr.File {
		fr, err := f.Open()
		require.NoError(t, err)

		newPartBuf := bytes.Buffer{}
		_, err = newPartBuf.ReadFrom(fr)
		require.NoError(t, err)

		assert.Equal(t, string(exp[i]), newPartBuf.String())
		i++
	}

	assert.Equal(t, len(exp), i)
}

func TestArchiveLines(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: lines
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch
	for i, e := range exp {
		p := service.NewMessage(e)
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		msg = append(msg, p)
	}

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	require.Equal(t, 5, batch.CtxCollapsedCount(batches[0][0].Context()))
	bBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, `hello world first part
hello world second part
third part
fourth
5`, string(bBytes))
}

func TestArchiveConcatenate(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: concatenate
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch
	for i, e := range exp {
		p := service.NewMessage(e)
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		msg = append(msg, p)
	}

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	require.Equal(t, 5, batch.CtxCollapsedCount(batches[0][0].Context()))
	bBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, `hello world first parthello world second partthird partfourth5`, string(bBytes))
}

func TestArchiveJSONArray(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: json_array
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["nested","array"]`),
		[]byte(`true`),
	}

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch
	for i, e := range exp {
		p := service.NewMessage(e)
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		msg = append(msg, p)
	}

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)

	require.Equal(t, 5, batch.CtxCollapsedCount(batches[0][0].Context()))
	bBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, `[{"foo":"bar"},5,"testing 123",["nested","array"],true]`, string(bBytes))
}

func TestArchiveEmpty(t *testing.T) {
	conf, err := archiveProcConfig().ParseYAML(`
format: json_array
`, nil)
	require.NoError(t, err)

	proc, err := newArchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	var msg service.MessageBatch

	batches, err := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, err)
	require.Len(t, batches, 0)
}
