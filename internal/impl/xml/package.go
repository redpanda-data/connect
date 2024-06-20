// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package xml is a temporary way to convert XML to JSON. This package is only
// necessary because github.com/clbanning/mxj has global configuration. If we
// are able to configure a decoder etc at the API level then this package can be
// removed.
package xml

import (
	"encoding/xml"

	"github.com/clbanning/mxj/v2"
	"golang.org/x/net/html/charset"
)

func init() {
	dec := xml.NewDecoder(nil)
	dec.Strict = false
	dec.CharsetReader = charset.NewReaderLabel
	mxj.CustomDecoder = dec
}

// ToMap parses a byte slice as XML and returns a generic structure that can be
// serialized to JSON.
func ToMap(xmlBytes []byte, cast bool) (map[string]any, error) {
	root, err := mxj.NewMapXml(xmlBytes, cast)
	if err != nil {
		return nil, err
	}
	return map[string]any(root), nil
}
