package codec

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noopCloser struct {
	io.Reader
}

func (n noopCloser) Close() error {
	return nil
}

func testReaderOrdered(t *testing.T, codec string, buf io.ReadCloser, expected ...string) {
	t.Run("acks ordered reads", func(t *testing.T) {
		ctor, err := GetReader(codec, NewReaderConfig())
		require.NoError(t, err)

		ack := errors.New("default err")

		r, err := ctor(buf, func(ctx context.Context, err error) error {
			ack = err
			return nil
		})
		require.NoError(t, err)

		for _, exp := range expected {
			p, ackFn, err := r.Next(context.Background())
			require.NoError(t, err)
			require.NoError(t, ackFn(context.Background(), nil))
			assert.Equal(t, exp, string(p.Get()))
		}

		_, _, err = r.Next(context.Background())
		assert.EqualError(t, err, "EOF")

		assert.NoError(t, r.Close(context.Background()))
		assert.NoError(t, ack)
	})
}

func testReaderUnordered(t *testing.T, codec string, buf io.ReadCloser, expected ...string) {
	t.Run("acks unordered reads", func(t *testing.T) {
		ctor, err := GetReader(codec, NewReaderConfig())
		require.NoError(t, err)

		ack := errors.New("default err")

		r, err := ctor(buf, func(ctx context.Context, err error) error {
			ack = err
			return nil
		})
		require.NoError(t, err)

		var ackFns []ReaderAckFn
		for _, exp := range expected {
			p, ackFn, err := r.Next(context.Background())
			require.NoError(t, err)
			ackFns = append(ackFns, ackFn)
			assert.Equal(t, exp, string(p.Get()))
		}

		_, _, err = r.Next(context.Background())
		assert.EqualError(t, err, "EOF")
		assert.NoError(t, r.Close(context.Background()))

		for _, ackFn := range ackFns {
			require.NoError(t, ackFn(context.Background(), nil))
		}

		assert.NoError(t, ack)
	})
}

func TestLinesReader(t *testing.T) {
	buf := noopCloser{bytes.NewReader([]byte("foo\nbar\nbaz"))}
	testReaderOrdered(t, "lines", buf, "foo", "bar", "baz")

	buf = noopCloser{bytes.NewReader([]byte("foo\nbar\nbaz"))}
	testReaderUnordered(t, "lines", buf, "foo", "bar", "baz")
}

func TestAllBytesReader(t *testing.T) {
	buf := noopCloser{bytes.NewReader([]byte("foo\nbar\nbaz"))}
	testReaderOrdered(t, "all-bytes", buf, "foo\nbar\nbaz")

	buf = noopCloser{bytes.NewReader([]byte("foo\nbar\nbaz"))}
	testReaderUnordered(t, "all-bytes", buf, "foo\nbar\nbaz")
}

func TestDelimReader(t *testing.T) {
	buf := noopCloser{bytes.NewReader([]byte("fooXbarXbaz"))}
	testReaderOrdered(t, "delim:X", buf, "foo", "bar", "baz")

	buf = noopCloser{bytes.NewReader([]byte("fooXbarXbaz"))}
	testReaderUnordered(t, "delim:X", buf, "foo", "bar", "baz")
}

func TestTarReader(t *testing.T) {
	input := []string{
		"first document",
		"second document",
		"third document",
	}

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for i := range input {
		hdr := &tar.Header{
			Name: fmt.Sprintf("testfile%v", i),
			Mode: 0600,
			Size: int64(len(input[i])),
		}

		err := tw.WriteHeader(hdr)
		require.NoError(t, err)

		_, err = tw.Write([]byte(input[i]))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())

	inputBytes := tarBuf.Bytes()

	buf := noopCloser{bytes.NewReader(inputBytes)}
	testReaderOrdered(t, "tar", buf, input...)

	buf = noopCloser{bytes.NewReader(inputBytes)}
	testReaderUnordered(t, "tar", buf, input...)
}

func TestTarGzipReader(t *testing.T) {
	input := []string{
		"first document",
		"second document",
		"third document",
	}

	var gzipBuf bytes.Buffer

	zw := gzip.NewWriter(&gzipBuf)
	tw := tar.NewWriter(zw)
	for i := range input {
		hdr := &tar.Header{
			Name: fmt.Sprintf("testfile%v", i),
			Mode: 0600,
			Size: int64(len(input[i])),
		}

		err := tw.WriteHeader(hdr)
		require.NoError(t, err)

		_, err = tw.Write([]byte(input[i]))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())

	inputBytes := gzipBuf.Bytes()

	buf := noopCloser{bytes.NewReader(inputBytes)}
	testReaderOrdered(t, "tar-gzip", buf, input...)

	buf = noopCloser{bytes.NewReader(inputBytes)}
	testReaderUnordered(t, "tar-gzip", buf, input...)
}
