package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestBloblangCounter(t *testing.T) {
	exec, err := bloblang.Parse(`
map map_wide_counter {
  root = counter(min: this.length())
}

root.a = counter()
root.b = counter(min: this.min)
root.c = counter(min: this.min, max: this.max)
root.d = "foo".apply("map_wide_counter")
root.e = "foobar".apply("map_wide_counter")
root.f = "f".apply("map_wide_counter")
`)
	require.NoError(t, err)

	v, err := exec.Query(map[string]any{
		"min": 10,
		"max": 20,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"a": int64(1),
		"b": int64(10),
		"c": int64(10),
		"d": int64(3),
		"e": int64(4),
		"f": int64(5),
	}, v)

	v, err = exec.Query(map[string]any{
		"min": 100,
		"max": 200,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"a": int64(2),
		"b": int64(11),
		"c": int64(11),
		"d": int64(6),
		"e": int64(7),
		"f": int64(8),
	}, v)

	for i := 0; i < 9; i++ {
		v, err = exec.Query(nil)
		require.NoError(t, err)

		vObj, ok := v.(map[string]any)
		require.True(t, ok)

		assert.Equal(t, int64(3+i), vObj["a"])
		assert.Equal(t, int64(12+i), vObj["b"])
		assert.Equal(t, int64(12+i), vObj["c"])
	}

	v, err = exec.Query(nil)
	require.NoError(t, err)

	vObj, ok := v.(map[string]any)
	require.True(t, ok)

	assert.Equal(t, int64(10), vObj["c"])
}

func TestBloblangCounterBadParams(t *testing.T) {
	exec, err := bloblang.Parse(`
root.a = counter(min: this.min, max: this.max)
`)
	require.NoError(t, err)

	_, err = exec.Query(map[string]any{
		"min": -1,
		"max": 20,
	})
	require.Error(t, err)

	_, err = exec.Query(map[string]any{
		"min": 5,
		"max": 2,
	})
	require.Error(t, err)

	_, err = exec.Query(nil)
	require.Error(t, err)
}
