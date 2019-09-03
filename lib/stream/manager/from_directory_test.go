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

package manager

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/stream"
	yaml "gopkg.in/yaml.v3"
)

func TestFromDirectory(t *testing.T) {
	testDir, err := ioutil.TempDir("", "streams_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	barDir := filepath.Join(testDir, "bar")
	if err = os.Mkdir(barDir, 0777); err != nil {
		t.Fatal(err)
	}

	fooPath := filepath.Join(testDir, "foo.json")
	barPath := filepath.Join(barDir, "test.yaml")

	fooConf := stream.NewConfig()
	fooConf.Input.Type = "TEST_FOO"

	barConf := stream.NewConfig()
	barConf.Input.Type = "TEST_BAR"

	expConfs := map[string]stream.Config{
		"foo":      fooConf,
		"bar_test": barConf,
	}

	var fooBytes []byte
	if fooBytes, err = json.Marshal(fooConf); err != nil {
		t.Fatal(err)
	}
	var barBytes []byte
	if barBytes, err = yaml.Marshal(barConf); err != nil {
		t.Fatal(err)
	}

	if err = ioutil.WriteFile(fooPath, fooBytes, 0666); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(barPath, barBytes, 0666); err != nil {
		t.Fatal(err)
	}

	var actConfs map[string]stream.Config
	if actConfs, err = LoadStreamConfigsFromDirectory(true, testDir); err != nil {
		t.Fatal(err)
	}

	var actKeys, expKeys []string
	for id := range actConfs {
		actKeys = append(actKeys, id)
	}
	sort.Strings(actKeys)
	for id := range expConfs {
		expKeys = append(expKeys, id)
	}
	sort.Strings(expKeys)

	if !reflect.DeepEqual(actKeys, expKeys) {
		t.Errorf("Wrong keys in loaded set: %v != %v", actKeys, expKeys)
	}

	if exp, act := "TEST_FOO", actConfs["foo"].Input.Type; exp != act {
		t.Errorf("Wrong value in loaded set: %v != %v", act, exp)
	}
	if exp, act := "TEST_BAR", actConfs["bar_test"].Input.Type; exp != act {
		t.Errorf("Wrong value in loaded set: %v != %v", act, exp)
	}
}
