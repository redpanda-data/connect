// Copyright (c) 2018 Ashley Jeffs
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

package message

import (
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

type ocMetadata struct {
	m        types.Metadata
	onChange func()
}

func onChangeMetadata(m types.Metadata, onChange func()) types.Metadata {
	return ocMetadata{
		m:        m,
		onChange: onChange,
	}
}

func (m ocMetadata) Copy() types.Metadata {
	return m.m.Copy()
}

func (m ocMetadata) Get(key string) string {
	return m.m.Get(key)
}

func (m ocMetadata) Set(key, value string) types.Metadata {
	m.onChange()
	return m.m.Set(key, value)
}

func (m ocMetadata) Delete(key string) types.Metadata {
	m.onChange()
	return m.m.Delete(key)
}

func (m ocMetadata) Iter(f func(k, v string) error) error {
	return m.m.Iter(f)
}

//------------------------------------------------------------------------------

type ocPart struct {
	p        types.Part
	onChange func()
}

func onChangePart(p types.Part, onChange func()) types.Part {
	return ocPart{
		p:        p,
		onChange: onChange,
	}
}

func (p ocPart) Copy() types.Part {
	return p.p.Copy()
}

func (p ocPart) DeepCopy() types.Part {
	return p.p.DeepCopy()
}

func (p ocPart) Get() []byte {
	return p.p.Get()
}

func (p ocPart) Metadata() types.Metadata {
	return onChangeMetadata(p.p.Metadata(), p.onChange)
}

func (p ocPart) JSON() (interface{}, error) {
	return p.p.JSON()
}

func (p ocPart) Set(data []byte) types.Part {
	p.p.Set(data)
	p.onChange()
	return p
}

func (p ocPart) SetMetadata(meta types.Metadata) types.Part {
	p.p.SetMetadata(meta)
	p.onChange()
	return p
}

func (p ocPart) SetJSON(jObj interface{}) error {
	p.onChange()
	return p.p.SetJSON(jObj)
}

//------------------------------------------------------------------------------
