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

package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Jeffail/gabs/v2"
)

func CheckTagsOfType(v reflect.Type, checkedTypes map[string]struct{}, t *testing.T) {
	tPath := v.PkgPath() + "." + v.Name()
	if _, exists := checkedTypes[tPath]; len(v.PkgPath()) > 0 && exists {
		return
	}
	checkedTypes[tPath] = struct{}{}

	switch v.Kind() {
	case reflect.Slice:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Map:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			jTag := field.Tag.Get("json")
			yTag := field.Tag.Get("yaml")

			if len(yTag) == 0 {
				t.Errorf("Empty field tag in type %v", tPath)
			}

			if strings.ToLower(yTag) != yTag {
				t.Errorf("Non-lower case field tag in type %v: %v", tPath, yTag)
			}

			if jTag != yTag {
				t.Errorf("Mismatched config tags in type %v: json(%v) != yaml(%v)", tPath, jTag, yTag)
			}

			CheckTagsOfType(field.Type, checkedTypes, t)
		}
	}
}

func TestConfigTags(t *testing.T) {
	v := reflect.TypeOf(New())

	checkedTypes := map[string]struct{}{}
	CheckTagsOfType(v, checkedTypes, t)
}

func TestExampleGen(t *testing.T) {
	conf := New()
	AddExamples(&conf, "files", "memory", "jmespath", "file")

	jBytes, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	gObj, err := gabs.ParseJSON(jBytes)
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := `"files"`, gObj.Path("input.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"memory"`, gObj.Path("buffer.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["jmespath","filter_parts"]`, gObj.Path("pipeline.processors.*.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["text","jmespath"]`, gObj.Path("pipeline.processors.*.filter_parts.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"file"`, gObj.Path("output.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}
}
