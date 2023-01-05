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
