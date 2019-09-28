// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestFilesDirectory(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_file_input_test")
	if err != nil {
		t.Fatal(err)
	}
	var tmpInnerDir string
	if tmpInnerDir, err = ioutil.TempDir(tmpDir, "benthos_inner"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	var tmpFile *os.File
	if tmpFile, err = ioutil.TempFile(tmpDir, "f1"); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte("foo")); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	if tmpFile, err = ioutil.TempFile(tmpInnerDir, "f2"); err != nil {
		t.Fatal(err)
	}
	if _, err = tmpFile.Write([]byte("bar")); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	exp := map[string]struct{}{
		"foo": {},
		"bar": {},
	}
	act := map[string]struct{}{}

	conf := NewFilesConfig()
	conf.Path = tmpDir

	var f Type
	if f, err = NewFiles(conf); err != nil {
		t.Fatal(err)
	}

	if err = f.Connect(); err != nil {
		t.Error(err)
	}

	var msg types.Message
	if msg, err = f.Read(); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
	}
	if msg, err = f.Read(); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
	}
	if _, err = f.Read(); err != types.ErrTypeClosed {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestFilesFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "f1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte("foo")); err != nil {
		t.Fatal(err)
	}
	if err = tmpFile.Close(); err != nil {
		t.Fatal(err)
	}

	exp := map[string]struct{}{
		"foo": {},
	}
	act := map[string]struct{}{}

	conf := NewFilesConfig()
	conf.Path = tmpFile.Name()

	var f *Files
	if f, err = NewFiles(conf); err != nil {
		t.Fatal(err)
	}

	if err = f.Connect(); err != nil {
		t.Error(err)
	}

	var msg types.Message
	var ackFn AsyncAckFn
	if msg, ackFn, err = f.ReadWithContext(context.Background()); err != nil {
		t.Error(err)
	} else {
		resStr := string(msg.Get(0).Get())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		act[resStr] = struct{}{}
		if err = ackFn(context.Background(), response.NewAck()); err != nil {
			t.Error(err)
		}
	}
	if _, err = f.Read(); err != types.ErrTypeClosed {
		t.Error(err)
	}

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestFilesBadPath(t *testing.T) {
	conf := NewFilesConfig()
	conf.Path = "fdgdfkte34%#@$%#$%KL@#K$@:L#$23k;32l;23"

	if _, err := NewFiles(conf); err == nil {
		t.Error("Expected error from bad path")
	}
}

//------------------------------------------------------------------------------
