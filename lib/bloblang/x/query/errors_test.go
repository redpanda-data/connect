package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeError(t *testing.T) {
	tests := map[string]struct {
		actual interface{}
		types  []ValueType
		exp    string
	}{
		"want num get str": {
			actual: "hello world",
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found string: hello world`,
		},
		"want num or bool get str": {
			actual: "hello world",
			types:  []ValueType{ValueNumber, ValueBool},
			exp:    `expected number or bool value, found string: hello world`,
		},
		"want num, bool or array get str": {
			actual: "hello world",
			types:  []ValueType{ValueNumber, ValueBool, ValueArray},
			exp:    `expected number, bool or array value, found string: hello world`,
		},
		"want num get bytes": {
			actual: []byte("foo"),
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found bytes`,
		},
		"want num get bool": {
			actual: false,
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found bool: false`,
		},
		"want num get array": {
			actual: []interface{}{"foo"},
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found array`,
		},
		"want num get object": {
			actual: map[string]interface{}{"foo": "bar"},
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found object`,
		},
		"want num get null": {
			actual: nil,
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found null`,
		},
		"want num get delete": {
			actual: Delete(nil),
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found delete`,
		},
		"want num get nothing": {
			actual: Nothing(nil),
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found nothing`,
		},
		"want num get unknown": {
			actual: []string{"unknown"},
			types:  []ValueType{ValueNumber},
			exp:    `expected number value, found unknown`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.exp, NewTypeError(test.actual, test.types...).Error())
		})
	}
}

func TestTypeMismatchError(t *testing.T) {
	tests := map[string]struct {
		left  interface{}
		right interface{}
		exp   string
	}{
		"string to number": {
			left:  "foo",
			right: 10.0,
			exp:   `found incomparable types string and number`,
		},
		"bool to array": {
			left:  false,
			right: []interface{}{"foo"},
			exp:   `found incomparable types bool and array`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.exp, NewTypeMismatch(test.left, test.right).Error())
		})
	}
}
