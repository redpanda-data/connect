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

package redpanda

import (
	"encoding/binary"
	"errors"
	"slices"
	"unsafe"
)

type transformHeader struct {
	key   string
	value []byte
}

func (h *transformHeader) deserialize(output []byte) (n int, err error) {
	var amt int
	h.key, amt, err = readSizedString(output)
	if err != nil {
		return
	}
	n = amt
	h.value, amt, err = readSizedCopy(output[n:])
	n += amt
	return
}

func (h *transformHeader) serialize(output []byte) int {
	nk := writeSizedString(h.key, output)
	if nk < 0 {
		return nk
	}
	nv := writeSized(h.value, output[nk:])
	if nv < 0 {
		return nv
	}
	return nk + nv
}

func (h *transformHeader) maxSize() int {
	return sizedLenString(h.key) + sizedLen(h.value)
}

//------------------------------------------------------------------------------

type transformMessage struct {
	timestamp   int64
	offset      int64
	key         []byte
	value       []byte
	headers     []transformHeader
	outputTopic *string
}

func (m *transformMessage) deserialize(output []byte) (n int, err error) {
	var amt int
	m.key, amt, err = readSizedCopy(output)
	if err != nil {
		return
	}
	n = amt
	m.value, amt, err = readSizedCopy(output[n:])
	n += amt
	if err != nil {
		return
	}
	var numHeaders int
	numHeaders, amt, err = readNum(output[n:])
	if err != nil {
		return
	}
	n += amt
	for i := 0; i < numHeaders; i += 1 {
		var h transformHeader
		amt, err = h.deserialize(output[n:])
		if err != nil {
			return
		}
		n += amt
		m.headers = append(m.headers, h)
	}
	return
}

func (m *transformMessage) maxSize() int {
	total := sizedLen(m.key)
	total += sizedLen(m.value)
	total += binary.MaxVarintLen64
	for _, h := range m.headers {
		total += h.maxSize()
	}
	return total
}

func (m *transformMessage) serialize(output []byte) int {
	var total int
	n := writeSized(m.key, output)
	if n < 0 {
		return n
	}
	total += n
	n = writeSized(m.value, output[total:])
	if n < 0 {
		return n
	}
	total += n
	n = writeNum(len(m.headers), output[total:])
	if n < 0 {
		return n
	}
	total += n
	for _, h := range m.headers {
		n := h.serialize(output[total:])
		if n < 0 {
			return n
		}
		total += n
	}
	return total
}

//------------------------------------------------------------------------------

type transformWriteOptions struct {
	topic string
}

const outputTopicKey = 0x01

func (o *transformWriteOptions) deserialize(output []byte) (int, error) {
	if len(output) == 0 {
		return 0, nil
	}
	if output[0] != outputTopicKey {
		return 0, errInvalidData
	}
	topic, n, err := readSizedString(output[1:])
	if err != nil {
		return 0, err
	}
	o.topic = topic
	return n + 1, nil
}

//------------------------------------------------------------------------------

func writeNum(n int, out []byte) int {
	if len(out) < binary.MaxVarintLen64 {
		return -1
	}
	return binary.PutVarint(out, int64(n))
}

func writeSized(b, out []byte) int {
	if len(out) < binary.MaxVarintLen64 {
		return -1
	}
	if b == nil {
		return binary.PutVarint(out, -1)
	}
	n := binary.PutVarint(out, int64(len(b)))
	if len(out) < len(b)+n {
		return -1
	}
	n += copy(out[n:], b)
	return n
}

func writeSizedString(s string, out []byte) int {
	return writeSized(unsafe.Slice(unsafe.StringData(s), len(s)), out)
}

func sizedLen(b []byte) int {
	return binary.MaxVarintLen64 + len(b)
}

func sizedLenString(b string) int {
	return binary.MaxVarintLen64 + len(b)
}

var errInvalidData = errors.New("unable to decode payload from Redpanda Data Transform")

func readNum(b []byte) (int, int, error) {
	n, amt := binary.Varint(b)
	if amt <= 0 {
		return 0, 0, errInvalidData
	}
	return int(n), amt, nil
}

func readSized(b []byte) ([]byte, int, error) {
	v, num := binary.Varint(b)
	if num <= 0 {
		return nil, 0, errInvalidData
	}
	if v < 0 {
		return nil, num, nil
	}
	b = b[num:]
	if int(v) > len(b) {
		return nil, 0, errInvalidData
	}
	return b[:v], num + int(v), nil
}

func readSizedCopy(b []byte) ([]byte, int, error) {
	b, amt, err := readSized(b)
	if err != nil {
		return b, amt, err
	}
	if b == nil {
		return b, amt, nil
	}
	return slices.Clone(b), amt, nil
}

func readSizedString(b []byte) (string, int, error) {
	s, amt, err := readSized(b)
	if err != nil {
		return "", amt, err
	}
	if s == nil {
		return "", amt, nil
	}
	return string(s), amt, nil
}
