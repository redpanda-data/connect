package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionSetWithout(t *testing.T) {
	setOne := AllFunctions
	setTwo := setOne.Without("uuid_v4")

	assert.Contains(t, setOne.List(), "uuid_v4")
	assert.NotContains(t, setTwo.List(), "uuid_v4")

	_, err := setOne.Init("uuid_v4")
	assert.NoError(t, err)

	_, err = setTwo.Init("uuid_v4")
	assert.EqualError(t, err, "unrecognised function 'uuid_v4'")

	_, err = setTwo.Init("timestamp_unix")
	assert.NoError(t, err)
}
