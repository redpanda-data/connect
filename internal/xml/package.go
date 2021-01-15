// Package xml is a temporary way to convert XML to JSON. This package is only
// necessary because github.com/clbanning/mxj has global configuration. If we
// are able to configure a decoder etc at the API level then this package can be
// removed.
package xml

import (
	"bytes"
	"encoding/xml"

	"github.com/clbanning/mxj"
	"golang.org/x/net/html/charset"
)

var defaultHTMLToEscape = []string{
	"b", "i", "u", "em", "strike", "sup", "small", "tt", "pre", "blockquote", "strong", "font",
}

func init() {
	dec := xml.NewDecoder(nil)
	dec.Strict = false
	dec.CharsetReader = charset.NewReaderLabel
	mxj.CustomDecoder = dec
}

// ToMap parses a byte slice as XML and returns a generic structure that can be
// serialized to JSON.
func ToMap(xmlBytes []byte) (map[string]interface{}, error) {
	root, err := mxj.NewMapXml(xmlBytes)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}(root), nil
}

// ToMapWithHTML parses a byte slice as XML while escaping HTML tags and returns a generic structure that can be
// serialized to JSON.
func ToMapWithHTML(xmlBytes []byte, htmlToEscape []string) (map[string]interface{}, error) {
	xmlBytes = preprocessHTML(xmlBytes, htmlToEscape)
	root, err := mxj.NewMapXml(xmlBytes)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}(root), nil
}

// preprocessHTML escapes the HTML tags from the content supplied into a format that will not break xml parsing
func preprocessHTML(data []byte, htmlToEscape []string) []byte {
	if len(htmlToEscape) <= 0 {
		htmlToEscape = defaultHTMLToEscape
	}

	for _, v := range htmlToEscape {
		data = bytes.ReplaceAll(data, []byte("<"+v+">"), []byte("&lt;"+v+"&gt;"))
		data = bytes.ReplaceAll(data, []byte("</"+v+">"), []byte("&lt;/"+v+"&gt;"))
	}

	return data
}
