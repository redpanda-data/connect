package message

// Contains underlying allocated data for messages.
type messageData struct {
	rawBytes []byte // Contents are always read-only
	err      error

	// Mutable when readOnlyStructured = false
	readOnlyStructured bool
	structured         any // Sometimes mutable

	// Mutable when readOnlyMeta = false
	readOnlyMeta bool
	metadata     map[string]any
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
		m.rawBytes = encodeJSON(m.structured)
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

	var err error
	m.structured, err = decodeJSON(m.rawBytes)
	return m.structured, err
}

func (m *messageData) AsStructuredMut() (any, error) {
	if m.readOnlyStructured {
		if m.structured != nil {
			m.structured = cloneGeneric(m.structured)
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
	var clonedMeta map[string]any
	if m.metadata != nil {
		clonedMeta = make(map[string]any, len(m.metadata))
		for k, v := range m.metadata {
			clonedMeta[k] = cloneGeneric(v)
		}
	}

	var bytesCopy []byte
	if len(m.rawBytes) > 0 {
		bytesCopy = make([]byte, len(m.rawBytes))
		copy(bytesCopy, m.rawBytes)
	}

	var structuredCopy any
	if m.structured != nil {
		structuredCopy = cloneGeneric(m.structured)
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

	var clonedMeta map[string]any
	if m.metadata != nil {
		clonedMeta = make(map[string]any, len(m.metadata))
		for k, v := range m.metadata {
			// NOTE: All metadata is store as mutable so no need to deep clone.
			clonedMeta[k] = v
		}
	}

	m.metadata = clonedMeta
	m.readOnlyMeta = false
}

func (m *messageData) MetaGetMut(key string) (any, bool) {
	if m.metadata == nil {
		return nil, false
	}
	s, exists := m.metadata[key]
	if !exists {
		return nil, false
	}
	return s, true
}

func (m *messageData) MetaSetMut(key string, value any) {
	m.writeableMeta()
	if m.metadata == nil {
		m.metadata = map[string]any{
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

func (m *messageData) MetaIterMut(f func(k string, v any) error) error {
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
