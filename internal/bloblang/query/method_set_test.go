package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMethodSetWithout(t *testing.T) {
	setOne := AllMethods
	setTwo := setOne.Without("explode")

	assert.Contains(t, setOne.List(), "explode")
	assert.NotContains(t, setTwo.List(), "explode")

	_, err := setOne.Init("explode", NewLiteralFunction(nil), "foo.bar")
	assert.NoError(t, err)

	_, err = setTwo.Init("explode", NewLiteralFunction(nil), "foo.bar")
	assert.EqualError(t, err, "unrecognised method 'explode'")

	_, err = setTwo.Init("map_each", NewLiteralFunction(nil), NewFieldFunction("foo"))
	assert.NoError(t, err)
}
