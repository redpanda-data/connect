// Package xml is a temporary way to convert XML to JSON. This package is only
// necessary because github.com/clbanning/mxj has global configuration. If we
// are able to configure a decoder etc at the API level then this package can be
// removed.
package xml

import (
	"encoding/xml"

	"github.com/clbanning/mxj"
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
func ToMap(xmlBytes []byte) (map[string]interface{}, error) {
	root, err := mxj.NewMapXml(xmlBytes)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}(root), nil
}
