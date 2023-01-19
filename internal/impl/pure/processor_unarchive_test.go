package pure

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestUnarchiveBadAlgo(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: does not exist
`, nil)
	require.NoError(t, err)

	_, err = newUnarchiveFromParsed(conf, service.MockResources())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestUnarchiveTar(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: tar
`, nil)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}
	expNames := []string{}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for i := range input {
		exp = append(exp, input[i])

		hdr := &tar.Header{
			Name: fmt.Sprintf("testfile%v", i),
			Mode: 0o600,
			Size: int64(len(input[i])),
		}
		expNames = append(expNames, hdr.Name)
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(input[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage(buf.Bytes()))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		key, exists := msgs[i].MetaGet("archive_filename")
		require.True(t, exists)
		assert.Equal(t, expNames[i], key)

		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveZip(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: zip
`, nil)
	require.NoError(t, err)

	input := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	exp := [][]byte{}
	expNames := []string{}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	for i := range input {
		exp = append(exp, input[i])

		name := fmt.Sprintf("testfile%v", i)
		expNames = append(expNames, name)
		if fw, err := zw.Create(name); err != nil {
			t.Fatal(err)
		} else if _, err := fw.Write(input[i]); err != nil {
			t.Fatal(err)
		}
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage(buf.Bytes()))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		key, exists := msgs[i].MetaGet("archive_filename")
		require.True(t, exists)
		assert.Equal(t, expNames[i], key)

		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveLines(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
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

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`hello world first part
hello world second part
third part
fourth
5`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveJSONDocuments(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: json_documents
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["root","is","an","array"]`),
		[]byte(`{"bar":"baz"}`),
		[]byte(`true`),
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`{"foo":"bar"} 5 "testing 123" ["root", "is", "an", "array"] {"bar": "baz"} true`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveJSONArray(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
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

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`[{"foo":"bar"},5,"testing 123",["nested","array"],true]`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveJSONMap(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: json_map
`, nil)
	require.NoError(t, err)

	exp := map[string]string{
		"a": `{"foo":"bar"}`,
		"b": `5`,
		"c": `"testing 123"`,
		"d": `["nested","array"]`,
		"e": `true`,
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`{"a":{"foo":"bar"},"b":5,"c":"testing 123","d":["nested","array"],"e":true}`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i := 0; i < len(exp); i++ {
		key, exists := msgs[i].MetaGet("archive_key")
		require.True(t, exists)

		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp[key], string(mBytes))
		delete(exp, key)
	}
}

func TestUnarchiveBinary(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: binary
`, nil)
	require.NoError(t, err)

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["nested","array"]`),
		[]byte(`true`),
	}
	testMsgBlob := message.SerializeBytes(exp)

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage(testMsgBlob))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(e), string(mBytes))
	}
}

func TestUnarchiveCSV(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: csv
`, nil)
	require.NoError(t, err)

	exp := []string{
		`{"color":"blue","id":"1","name":"foo"}`,
		`{"color":"green","id":"2","name":"bar"}`,
		`{"color":"red","id":"3","name":"baz"}`,
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`id,name,color
1,foo,blue
2,bar,green
3,baz,red`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(mBytes))
	}
}

func TestUnarchiveCSVCustom(t *testing.T) {
	conf, err := unarchiveProcConfig().ParseYAML(`
format: csv:|
`, nil)
	require.NoError(t, err)

	exp := []string{
		`{"color":"blue","id":"1","name":"foo"}`,
		`{"color":"green","id":"2","name":"bar"}`,
		`{"color":"red","id":"3","name":"baz"}`,
	}

	proc, err := newUnarchiveFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	msgs, res := proc.Process(context.Background(), service.NewMessage([]byte(
		`id|name|color
1|foo|blue
2|bar|green
3|baz|red`,
	)))
	require.NoError(t, res)
	require.Len(t, msgs, len(exp))

	for i, e := range exp {
		mBytes, err := msgs[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(mBytes))
	}
}
