package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// Contains underlying allocated data for messages.
type messageData struct {
	rawBytes []byte // Contents are always read-only
	err      error

	// Mutable when readOnlyStructured = false
	readOnlyStructured bool
	structured         any // Sometimes mutable

	// Mutable when readOnlyMeta = false
	readOnlyMeta bool
	metadata     map[string]string
}

func newMessageBytes(content []byte) *messageData {
	return &messageData{
		rawBytes: content,
		metadata: nil,
		err:      nil,
	}
}

func (m *messageData) SetBytes(d []byte) {
	m.rawBytes = d
	m.structured = nil
}

func (m *messageData) AsBytes() []byte {
	if len(m.rawBytes) == 0 && m.structured != nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(m.structured); err != nil {
			return nil
		}
		if buf.Len() > 1 {
			m.rawBytes = buf.Bytes()[:buf.Len()-1]
		}
	}
	return m.rawBytes
}

func (m *messageData) SetStructured(jObj any) {
	m.rawBytes = nil
	if jObj == nil {
		m.rawBytes = []byte(`null`)
		return
	}
	m.rawBytes = nil
	m.structured = jObj
	m.readOnlyStructured = true
}

func (m *messageData) SetStructuredMut(jObj any) {
	m.SetStructured(jObj)
	m.readOnlyStructured = false
}

func (m *messageData) AsStructured() (any, error) {
	if m.structured != nil {
		return m.structured, nil
	}

	if len(m.rawBytes) == 0 {
		return nil, ErrMessagePartNotExist // TODO: Need this?
	}

	dec := json.NewDecoder(bytes.NewReader(m.rawBytes))
	if useNumber {
		dec.UseNumber()
	}

	if err := dec.Decode(&m.structured); err != nil {
		return nil, err
	}

	var dummy json.RawMessage
	if err := dec.Decode(&dummy); err == io.EOF {
		return m.structured, nil
	}

	m.structured = nil
	err := dec.Decode(&dummy)
	if err == nil || err == io.EOF {
		err = errors.New("message contains multiple valid documents")
	}
	return nil, err
}

func (m *messageData) AsStructuredMut() (any, error) {
	if m.readOnlyStructured {
		if m.structured != nil {
			var err error
			if m.structured, err = cloneGeneric(m.structured); err != nil {
				return nil, err
			}
		}
		m.readOnlyStructured = true
	}
	v, err := m.AsStructured()
	if err != nil {
		return nil, err
	}

	// Bytes need resetting as our structured form may change
	m.rawBytes = nil
	return v, nil
}

// ShallowCopy returns a copy of the message data that can be mutated without
// mutating the original message contents (metadata and structured data).
func (m *messageData) ShallowCopy() *messageData {
	return &messageData{
		rawBytes: m.rawBytes,
		err:      m.err,

		readOnlyStructured: true,
		structured:         m.structured,

		readOnlyMeta: true,
		metadata:     m.metadata,
	}
}

// DeepCopy returns a copy of the message data that can be mutated without
// mutating the original message contents (metadata and structured data), and
// all underlying reference types are deeply copied to eliminate any parental
// ownership.
//
// This is worth doing on values persisted outside of the lifetime of a
// transaction unless some other strategy is used for persistence.
func (m *messageData) DeepCopy() *messageData {
	var clonedMeta map[string]string
	if m.metadata != nil {
		clonedMeta = make(map[string]string, len(m.metadata))
		for k, v := range m.metadata {
			clonedMeta[k] = v
		}
	}

	var bytesCopy []byte
	if len(m.rawBytes) > 0 {
		bytesCopy = make([]byte, len(m.rawBytes))
		copy(bytesCopy, m.rawBytes)
	}

	var structuredCopy any
	if m.structured != nil {
		structuredCopy, _ = CopyJSON(m.structured)
	}

	return &messageData{
		rawBytes:   bytesCopy,
		err:        m.err,
		structured: structuredCopy,
		metadata:   clonedMeta,
	}
}

func (m *messageData) IsEmpty() bool {
	return len(m.rawBytes) == 0 && m.structured == nil
}

func (m *messageData) writeableMeta() {
	if !m.readOnlyMeta {
		return
	}

	var clonedMeta map[string]string
	if m.metadata != nil {
		clonedMeta = make(map[string]string, len(m.metadata))
		for k, v := range m.metadata {
			clonedMeta[k] = v
		}
	}

	m.metadata = clonedMeta
	m.readOnlyMeta = false
}

func (m *messageData) MetaGet(key string) (string, bool) {
	if m.metadata == nil {
		return "", false
	}
	s, exists := m.metadata[key]
	return s, exists
}

func (m *messageData) MetaSet(key, value string) {
	m.writeableMeta()
	if m.metadata == nil {
		m.metadata = map[string]string{
			key: value,
		}
		return
	}
	m.metadata[key] = value
}

func (m *messageData) MetaDelete(key string) {
	m.writeableMeta()
	delete(m.metadata, key)
}

func (m *messageData) MetaIter(f func(k, v string) error) error {
	for ak, av := range m.metadata {
		if err := f(ak, av); err != nil {
			return err
		}
	}
	return nil
}

func (m *messageData) ErrorGet() error {
	return m.err
}

func (m *messageData) ErrorSet(err error) {
	m.err = err
}
