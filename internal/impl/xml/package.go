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
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

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

// ToMapPreserveNS parses a byte slice as XML with namespace prefixes preserved
// on element and attribute keys (e.g. "<dc:title>" becomes the key "dc:title")
// and with xmlns declarations retained as attributes, so the original XML is
// reconstructable from the resulting JSON. The output shape otherwise matches
// [ToMap]: attributes are prefixed with "-", mixed text content uses the
// "#text" key, and repeated elements are collected into arrays.
func ToMapPreserveNS(xmlBytes []byte, cast bool) (map[string]any, error) {
	dec := xml.NewDecoder(bytes.NewReader(xmlBytes))
	dec.Strict = false
	dec.CharsetReader = charset.NewReaderLabel

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil, fmt.Errorf("xml: no root element found")
		}
		if err != nil {
			return nil, err
		}
		if se, ok := tok.(xml.StartElement); ok {
			key, val, err := parseElementNS(dec, se, map[string]string{}, cast)
			if err != nil {
				return nil, err
			}
			return map[string]any{key: val}, nil
		}
	}
}

// collectPrefixDecls records URI→prefix mappings from xmlns:* attributes on an
// element into the provided scope map.
func collectPrefixDecls(se xml.StartElement, scope map[string]string) {
	for _, a := range se.Attr {
		if a.Name.Space == "xmlns" {
			scope[a.Value] = a.Name.Local
		}
	}
}

// qnameWithPrefix returns "prefix:local" when a prefix is known for the
// namespace URI; when the namespace was never bound via xmlns, Go's decoder
// leaves Name.Space as the raw prefix string which is used directly.
func qnameWithPrefix(n xml.Name, scope map[string]string) string {
	if n.Space == "" {
		return n.Local
	}
	if p, ok := scope[n.Space]; ok {
		return p + ":" + n.Local
	}
	return n.Space + ":" + n.Local
}

func parseElementNS(dec *xml.Decoder, se xml.StartElement, parent map[string]string, cast bool) (string, any, error) {
	scope := make(map[string]string, len(parent)+len(se.Attr))
	for k, v := range parent {
		scope[k] = v
	}
	collectPrefixDecls(se, scope)

	out := map[string]any{}
	for _, a := range se.Attr {
		var key string
		isNSDecl := false
		switch {
		case a.Name.Space == "xmlns":
			key = "-xmlns:" + a.Name.Local
			isNSDecl = true
		case a.Name.Space == "" && a.Name.Local == "xmlns":
			key = "-xmlns"
			isNSDecl = true
		default:
			key = "-" + qnameWithPrefix(a.Name, scope)
		}
		if isNSDecl {
			out[key] = a.Value
		} else {
			out[key] = castString(a.Value, cast)
		}
	}

	var text strings.Builder
	for {
		tok, err := dec.Token()
		if err != nil {
			return "", nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			k, v, err := parseElementNS(dec, t, scope, cast)
			if err != nil {
				return "", nil, err
			}
			if existing, ok := out[k]; ok {
				if arr, isArr := existing.([]any); isArr {
					out[k] = append(arr, v)
				} else {
					out[k] = []any{existing, v}
				}
			} else {
				out[k] = v
			}
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			s := strings.TrimSpace(text.String())
			key := qnameWithPrefix(se.Name, scope)
			if len(out) == 0 {
				return key, castString(s, cast), nil
			}
			if s != "" {
				out["#text"] = castString(s, cast)
			}
			return key, out, nil
		}
	}
}

// castString mirrors clbanning/mxj's default cast order when casting is
// enabled: int → float → bool, with NaN/Inf left as strings.
func castString(s string, cast bool) any {
	if !cast || s == "" {
		return s
	}
	switch strings.ToLower(s) {
	case "nan", "inf", "-inf":
		return s
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return u
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if len(s) < 6 {
		switch s[:1] {
		case "t", "T", "f", "F":
			if b, err := strconv.ParseBool(s); err == nil {
				return b
			}
		}
	}
	return s
}
