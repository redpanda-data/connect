// Copyright (c) 2019 Ashley Jeffs
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

package test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/processor"
)

func initTestFiles(files map[string]string) (string, error) {
	testDir, err := ioutil.TempDir("", "benthos_config_test_test")
	if err != nil {
		return "", err
	}

	for k, v := range files {
		fp := filepath.Join(testDir, k)
		if err = os.MkdirAll(filepath.Dir(fp), 0777); err != nil {
			return "", err
		}
		if err = ioutil.WriteFile(fp, []byte(v), 0777); err != nil {
			return "", err
		}
	}

	return testDir, nil
}

func TestProcessorsProviderErrors(t *testing.T) {
	files := map[string]string{
		"config1.yaml": `
this isnt valid yaml
		nah
		what is even happening here?`,
		"config2.yaml": `
pipeline:
  processors:
  - type: text`,
		"config3.yaml": `
pipeline:
  processors:
  - type: doesnotexist`,
	}

	testDir, err := initTestFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = NewProcessorsProvider(filepath.Join(testDir, "doesnotexist.yaml")).Provide("/pipeline/processors", nil); err == nil {
		t.Error("Expected error from bad filepath")
	}
	if _, err = NewProcessorsProvider(filepath.Join(testDir, "config1.yaml")).Provide("/pipeline/processors", nil); err == nil {
		t.Error("Expected error from bad config file")
	}
	if _, err = NewProcessorsProvider(filepath.Join(testDir, "config2.yaml")).Provide("/not/a/valid/path", nil); err == nil {
		t.Error("Expected error from bad processors path")
	}
	if _, err = NewProcessorsProvider(filepath.Join(testDir, "config3.yaml")).Provide("/pipeline/processors", nil); err == nil {
		t.Error("Expected error from bad processor type")
	}
}

func TestProcessorsProvider(t *testing.T) {
	files := map[string]string{
		"config1.yaml": `
resources:
  caches:
    foocache:
      memory: {}

pipeline:
  processors:
  - metadata:
      operator: set
      key: foo
      value: ${FOO_VAR:defaultvalue}
  - cache:
      cache: foocache
      operator: set
      key: defaultkey
      value: ${!metadata:foo}
  - cache:
      cache: foocache
      operator: get
      key: defaultkey
  - text:
      operator: to_upper`,

		"config2.yaml": `
resources:
  caches:
    foocache:
      memory: {}

pipeline:
  processors:
    $ref: ./config1.yaml#/pipeline/processors`,
	}

	testDir, err := initTestFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	provider := NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("/pipeline/processors", nil)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 4, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	msgs, res := processor.ExecuteAll(procs, message.New([][]byte{[]byte("hello world")}))
	if res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := "DEFAULTVALUE", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	if procs, err = provider.Provide("/pipeline/processors", map[string]string{
		"FOO_VAR": "newvalue",
	}); err != nil {
		t.Fatal(err)
	}
	if exp, act := 4, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	if msgs, res = processor.ExecuteAll(procs, message.New([][]byte{[]byte("hello world")})); res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := "NEWVALUE", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	provider = NewProcessorsProvider(filepath.Join(testDir, "config2.yaml"))
	if procs, err = provider.Provide("/pipeline/processors", map[string]string{
		"FOO_VAR": "thirdvalue",
	}); err != nil {
		t.Fatal(err)
	}
	if exp, act := 4, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	if msgs, res = processor.ExecuteAll(procs, message.New([][]byte{[]byte("hello world")})); res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := "THIRDVALUE", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	if procs, err = provider.Provide("/pipeline/processors/3", nil); err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	if msgs, res = processor.ExecuteAll(procs, message.New([][]byte{[]byte("hello world")})); res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := "HELLO WORLD", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}
