// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

package text

//------------------------------------------------------------------------------

// InterpolatedString holds a string that potentially has interpolation
// functions. Each time Get is called any functions are replaced with their
// evaluated results in the string.
type InterpolatedString struct {
	str         string
	strBytes    []byte
	interpolate bool
}

// Get evaluates functions within the original string and returns the result.
func (i *InterpolatedString) Get(msg Message) string {
	if !i.interpolate {
		return i.str
	}
	return string(ReplaceFunctionVariables(msg, i.strBytes))
}

// NewInterpolatedString returns a type that evaluates function interpolations
// on a provided string each time Get is called.
func NewInterpolatedString(str string) *InterpolatedString {
	strI := &InterpolatedString{
		str: str,
	}
	if strBytes := []byte(str); ContainsFunctionVariables(strBytes) {
		strI.strBytes = strBytes
		strI.interpolate = true
	}
	return strI
}

//------------------------------------------------------------------------------

// InterpolatedBytes holds a byte slice that potentially has interpolation
// functions. Each time Get is called any functions are replaced with their
// evaluated results in the byte slice.
type InterpolatedBytes struct {
	v           []byte
	interpolate bool
}

// Get evaluates functions within the byte slice and returns the result.
func (i *InterpolatedBytes) Get(msg Message) []byte {
	if !i.interpolate {
		return i.v
	}
	return ReplaceFunctionVariables(msg, i.v)
}

// NewInterpolatedBytes returns a type that evaluates function interpolations
// on a provided byte slice each time Get is called.
func NewInterpolatedBytes(v []byte) *InterpolatedBytes {
	return &InterpolatedBytes{
		v:           v,
		interpolate: ContainsFunctionVariables(v),
	}
}

//------------------------------------------------------------------------------
