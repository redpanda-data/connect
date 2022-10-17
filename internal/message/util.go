package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"time"
)

var useNumber = true

func init() {
	if os.Getenv("BENTHOS_USE_NUMBER") == "false" {
		useNumber = false
	}
}

//------------------------------------------------------------------------------

// GetAllBytes returns a 2D byte slice representing the raw byte content of the
// parts of a message.
func GetAllBytes(m Batch) [][]byte {
	if len(m) == 0 {
		return nil
	}
	parts := make([][]byte, len(m))
	_ = m.Iter(func(i int, p *Part) error {
		parts[i] = p.AsBytes()
		return nil
	})
	return parts
}

//------------------------------------------------------------------------------

func decodeJSON(rawBytes []byte) (structured any, err error) {
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	if useNumber {
		dec.UseNumber()
	}

	if err = dec.Decode(&structured); err != nil {
		return
	}

	var dummy json.RawMessage
	if err = dec.Decode(&dummy); errors.Is(err, io.EOF) {
		err = nil
		return
	}

	structured = nil
	if err = dec.Decode(&dummy); err == nil || err == io.EOF {
		err = errors.New("message contains multiple valid documents")
	}
	return
}

func encodeJSON(d any) (rawBytes []byte) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(d); err != nil {
		return nil
	}
	if buf.Len() > 1 {
		rawBytes = buf.Bytes()[:buf.Len()-1]
	}
	return
}

// Copy of query.IToString
func metaToString(i any) string {
	switch t := i.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case json.Number:
		return t.String()
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		return t.Format(time.RFC3339Nano)
	case nil:
		return `null`
	}
	// Last resort
	return string(encodeJSON(i))
}

//------------------------------------------------------------------------------

func cloneMap(oldMap map[string]any) map[string]any {
	newMap := make(map[string]any, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = cloneGeneric(v)
	}
	return newMap
}

func cloneCheekyMap(oldMap map[any]any) map[any]any {
	newMap := make(map[any]any, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = cloneGeneric(v)
	}
	return newMap
}

func cloneSlice(oldSlice []any) []any {
	newSlice := make([]any, len(oldSlice))
	for i, v := range oldSlice {
		newSlice[i] = cloneGeneric(v)
	}
	return newSlice
}

// cloneGeneric is a utility function that recursively copies a generic
// structure usually resulting from a JSON parse.
func cloneGeneric(root any) any {
	switch t := root.(type) {
	case map[string]any:
		return cloneMap(t)
	case map[any]any:
		return cloneCheekyMap(t)
	case []any:
		return cloneSlice(t)
	default:
		// Oops, this means we have 'dirty' types within the object, we pass
		// these through uncloned and hope that the author knows what they're
		// doing.
		return root
	}
}

// CopyJSON recursively creates a deep copy of a JSON structure extracted from a
// message part.
func CopyJSON(root any) any {
	return cloneGeneric(root)
}
