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

package main

import (
	"reflect"
	"testing"
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

			if jTag != yTag {
				t.Errorf("Mismatched config tags in type %v: json(%v) != yaml(%v)", tPath, jTag, yTag)
			}

			CheckTagsOfType(field.Type, checkedTypes, t)
		}
	}
}

func TestConfigTags(t *testing.T) {
	v := reflect.TypeOf(NewConfig())

	checkedTypes := map[string]struct{}{}
	CheckTagsOfType(v, checkedTypes, t)
}
