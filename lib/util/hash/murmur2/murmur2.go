// Copyright (c) 2014 Ashley Jeffs
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

package murmur2

import "hash"

type murmur2 struct {
	data   []byte
	cached *uint32
}

// New32 creates a murmur 2 based hash.Hash32 implementation.
func New32() hash.Hash32 {
	return &murmur2{
		data: make([]byte, 0),
	}
}

// Write a slice of data to the hasher.
func (mur *murmur2) Write(p []byte) (n int, err error) {
	mur.data = append(mur.data, p...)
	mur.cached = nil
	return len(p), nil
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (mur *murmur2) Sum(b []byte) []byte {
	v := mur.Sum32()
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// Reset resets the Hash to its initial state.
func (mur *murmur2) Reset() {
	mur.data = mur.data[0:0]
	mur.cached = nil
}

// Size returns the number of bytes Sum will return.
func (mur *murmur2) Size() int {
	return 4
}

// BlockSize returns the hash's underlying block size.
// The Write method must be able to accept any amount
// of data, but it may operate more efficiently if all writes
// are a multiple of the block size.
func (mur *murmur2) BlockSize() int {
	return 4
}

const (
	seed uint32 = uint32(0x9747b28c)
	m    int32  = int32(0x5bd1e995)
	r    uint32 = uint32(24)
)

func (mur *murmur2) Sum32() uint32 {
	if mur.cached != nil {
		return *mur.cached
	}

	length := int32(len(mur.data))

	h := int32(seed ^ uint32(length))
	length4 := length / 4

	for i := int32(0); i < length4; i++ {
		i4 := i * 4
		k := int32(mur.data[i4+0]&0xff) +
			(int32(mur.data[i4+1]&0xff) << 8) +
			(int32(mur.data[i4+2]&0xff) << 16) +
			(int32(mur.data[i4+3]&0xff) << 24)
		k *= m
		k ^= int32(uint32(k) >> r)
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= int32(mur.data[(length & ^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= int32(mur.data[(length & ^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= int32(mur.data[length & ^3] & 0xff)
		h *= m
	}

	h ^= int32(uint32(h) >> 13)
	h *= m
	h ^= int32(uint32(h) >> 15)

	cached := uint32(h)
	mur.cached = &cached
	return cached
}
