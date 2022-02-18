package processor

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/require"
)

func TestArchiveBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "does not exist"

	_, err := newArchive(conf.Archive, mock.NewManager())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestArchiveTar(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "tar"
	conf.Archive.Path = "foo-${!meta(\"path\")}"

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.QuickBatch(exp)
	_ = msg.Iter(func(i int, p *message.Part) error {
		p.MetaSet("path", fmt.Sprintf("bar%v", i))
		return nil
	})
	msgs, res := proc.ProcessBatch(context.Background(), nil, msg)
	if len(msgs) != 1 {
		t.Error("Archive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if msgs[0].Len() != 1 {
		t.Fatal("More parts than expected")
	}

	act := [][]byte{}

	buf := bytes.NewBuffer(msgs[0].Get(0).Get())
	tr := tar.NewReader(buf)
	i := 0
	for {
		var hdr *tar.Header
		hdr, err = tr.Next()
		if err == io.EOF {
			// end of tar archive
			break
		}
		if err != nil {
			t.Fatal(err)
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(tr); err != nil {
			t.Fatal(err)
		}

		act = append(act, newPartBuf.Bytes())
		if exp, act := fmt.Sprintf("foo-bar%v", i), hdr.FileInfo().Name(); exp != act {
			t.Errorf("Wrong filename: %v != %v", act, exp)
		}
		i++
	}

	require.Equal(t, 5, batch.CollapsedCount(msgs[0].Get(0)))
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestArchiveZip(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "zip"

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), nil, message.QuickBatch(exp))
	if len(msgs) != 1 {
		t.Error("Archive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if msgs[0].Len() != 1 {
		t.Fatal("More parts than expected")
	}

	act := [][]byte{}

	buf := bytes.NewReader(msgs[0].Get(0).Get())
	zr, err := zip.NewReader(buf, int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range zr.File {
		fr, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}

		newPartBuf := bytes.Buffer{}
		if _, err = newPartBuf.ReadFrom(fr); err != nil {
			t.Fatal(err)
		}

		act = append(act, newPartBuf.Bytes())
	}

	require.Equal(t, 5, batch.CollapsedCount(msgs[0].Get(0)))
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestArchiveLines(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "lines"

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), nil, message.QuickBatch([][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}))
	if len(msgs) != 1 {
		t.Error("Archive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if msgs[0].Len() != 1 {
		t.Fatal("More parts than expected")
	}

	require.Equal(t, 5, batch.CollapsedCount(msgs[0].Get(0)))
	exp := [][]byte{
		[]byte(`hello world first part
hello world second part
third part
fourth
5`),
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestArchiveConcatenate(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "concatenate"

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), nil, message.QuickBatch([][]byte{
		{},
		{0},
		{1, 2},
		{3, 4, 5},
		{6},
	}))
	if len(msgs) != 1 {
		t.Error("Archive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if msgs[0].Len() != 1 {
		t.Fatal("More parts than expected")
	}

	require.Equal(t, 5, batch.CollapsedCount(msgs[0].Get(0)))
	exp := [][]byte{
		{0, 1, 2, 3, 4, 5, 6},
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestArchiveJSONArray(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "json_array"

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessBatch(context.Background(), nil, message.QuickBatch([][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["nested","array"]`),
		[]byte(`true`),
	}))
	if len(msgs) != 1 {
		t.Error("Archive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if msgs[0].Len() != 1 {
		t.Fatal("More parts than expected")
	}

	require.Equal(t, 5, batch.CollapsedCount(msgs[0].Get(0)))
	exp := [][]byte{[]byte(
		`[{"foo":"bar"},5,"testing 123",["nested","array"],true]`,
	)}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestArchiveBinary(t *testing.T) {
	conf := NewConfig()
	conf.Archive.Format = "binary"

	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Error(err)
		return
	}

	testMsg := message.QuickBatch([][]byte{[]byte("hello"), []byte("world")})
	testMsgBlob := message.ToBytes(testMsg)

	if msgs, _ := proc.ProcessBatch(context.Background(), nil, testMsg); len(msgs) == 1 {
		if lParts := msgs[0].Len(); lParts != 1 {
			t.Errorf("Wrong number of parts returned: %v != %v", lParts, 1)
		}
		if !reflect.DeepEqual(testMsgBlob, msgs[0].Get(0).Get()) {
			t.Errorf("Returned message did not match: %s != %s", msgs[0].Get(0).Get(), testMsgBlob)
		}
	} else {
		t.Error("Failed on good message")
	}
}

func TestArchiveEmpty(t *testing.T) {
	conf := NewConfig()
	proc, err := newArchive(conf.Archive, mock.NewManager())
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessBatch(context.Background(), nil, message.QuickBatch([][]byte{}))
	if len(msgs) != 0 {
		t.Error("Expected failure with zero part message")
	}
}
