package processor

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestUnarchiveBadAlgo(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "does not exist"

	testLog := log.Noop()

	_, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad algo")
	}
}

func TestUnarchiveTar(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "tar"

	testLog := log.Noop()

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

	input = [][]byte{buf.Bytes()}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Errorf("Unarchive failed: %v", res)
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
	for i := 0; i < msgs[0].Len(); i++ {
		if name := msgs[0].Get(i).MetaGet("archive_filename"); name != expNames[i] {
			t.Errorf("Unexpected name %d: %s != %s", i, name, expNames[i])
		}
	}
}

func TestUnarchiveZip(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "zip"

	testLog := log.Noop()

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

	input = [][]byte{buf.Bytes()}

	if reflect.DeepEqual(input, exp) {
		t.Fatal("Input and exp output are the same")
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch(input))
	if len(msgs) != 1 {
		t.Errorf("Unarchive failed: %v", res)
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
	for i := 0; i < msgs[0].Len(); i++ {
		if name := msgs[0].Get(i).MetaGet("archive_filename"); name != expNames[i] {
			t.Errorf("Unexpected name %d: %s != %s", i, name, expNames[i])
		}
	}
}

func TestUnarchiveLines(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "lines"

	testLog := log.Noop()

	exp := [][]byte{
		[]byte("hello world first part"),
		[]byte("hello world second part"),
		[]byte("third part"),
		[]byte("fourth"),
		[]byte("5"),
	}

	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(`hello world first part
hello world second part
third part
fourth
5`),
	}))
	if len(msgs) != 1 {
		t.Error("Unarchive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestUnarchiveJSONDocuments(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "json_documents"

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["root","is","an","array"]`),
		[]byte(`{"bar":"baz"}`),
		[]byte(`true`),
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(`{"foo":"bar"} 5 "testing 123" ["root", "is", "an", "array"] {"bar": "baz"} true`),
	}))
	if len(msgs) != 1 {
		t.Error("Unarchive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestUnarchiveJSONArray(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "json_array"

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["nested","array"]`),
		[]byte(`true`),
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(`[{"foo":"bar"},5,"testing 123",["nested","array"],true]`),
	}))
	if len(msgs) != 1 {
		t.Error("Unarchive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected output: %s != %s", act, exp)
	}
}

func TestUnarchiveJSONMap(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "json_map"

	exp := [][]byte{
		[]byte(`{"foo":"bar"}`),
		[]byte(`5`),
		[]byte(`"testing 123"`),
		[]byte(`["nested","array"]`),
		[]byte(`true`),
	}
	expKeys := []string{
		"a", "b", "c", "d", "e",
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(`{"a":{"foo":"bar"},"b":5,"c":"testing 123","d":["nested","array"],"e":true}`),
	}))
	if len(msgs) != 1 {
		t.Error("Unarchive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	} else if msgs[0].Len() != len(exp) {
		t.Errorf("Incorrect number of messages: %d != %d", msgs[0].Len(), len(exp))
	}

	// we need to be careful of the random order the map will be generated in
	// so we can't just test for byte equivalence of the whole array
	found := make([]bool, msgs[0].Len())
	for i := 0; i < msgs[0].Len(); i++ {
		key := msgs[0].Get(i).MetaGet("archive_key")
		seq := -1
		for j := 0; j < len(expKeys); j++ {
			if expKeys[j] == key {
				seq = j
			}
		}
		if seq < 0 {
			t.Errorf("Unexpected output: incorrect key %s found in position %d", key, i)
		}
		if found[seq] {
			t.Errorf("Unexpected output: duplicate key %s found in position %d", key, i)
		}
		found[seq] = true
		if act := msgs[0].Get(i).Get(); !reflect.DeepEqual(exp[seq], act) {
			t.Errorf("Unexpected output: %s != %s", act, exp[seq])
		}
	}
	for i := 0; i < msgs[0].Len(); i++ {
		if !found[i] {
			t.Errorf("Missing output: message for key %s not found", expKeys[i])
		}
	}
}

func TestUnarchiveBinary(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "binary"

	testLog := log.Noop()
	proc, err := New(conf, mock.NewManager(), testLog, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessMessage(
		message.QuickBatch([][]byte{[]byte("wat this isnt good")}),
	)
	if exp, act := 1, len(msgs); exp != act {
		t.Fatalf("Wrong count: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count: %v != %v", act, exp)
	}
	if !HasFailed(msgs[0].Get(0)) {
		t.Error("Expected fail")
	}

	testMsg := message.QuickBatch([][]byte{[]byte("hello"), []byte("world")})
	testMsgBlob := message.ToBytes(testMsg)

	if msgs, _ := proc.ProcessMessage(message.QuickBatch([][]byte{testMsgBlob})); len(msgs) == 1 {
		if !reflect.DeepEqual(message.GetAllBytes(testMsg), message.GetAllBytes(msgs[0])) {
			t.Errorf("Returned message did not match: %v != %v", msgs, testMsg)
		}
	} else {
		t.Error("Failed on good message")
	}
}

func TestUnarchiveCSV(t *testing.T) {
	conf := NewConfig()
	conf.Type = "unarchive"
	conf.Unarchive.Format = "csv"

	exp := []interface{}{
		map[string]interface{}{"id": "1", "name": "foo", "color": "blue"},
		map[string]interface{}{"id": "2", "name": "bar", "color": "green"},
		map[string]interface{}{"id": "3", "name": "baz", "color": "red"},
	}

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{
		[]byte(strings.Join([]string{
			`id,name,color`,
			`1,foo,blue`,
			`2,bar,green`,
			`3,baz,red`,
		}, "\n")),
	}))
	if len(msgs) != 1 {
		t.Error("Unarchive failed")
	} else if res != nil {
		t.Errorf("Expected nil response: %v", res)
	}

	if msgs[0].Len() != len(exp) {
		t.Error("Unexpected output: wrong number of items")
		return
	}

	for i := 0; i < len(exp); i++ {
		expPart := exp[i]
		actPart, err := msgs[0].Get(i).JSON()
		if err != nil {
			t.Errorf("Unexpected json error: %v", err)
		}

		if !reflect.DeepEqual(expPart, actPart) {
			t.Errorf("Unexpected output: %v != %v", actPart, expPart)
		}
	}
}
