package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldInterpolatedStringList(t *testing.T) {
	conf := `
listfield:
  - hello ${! json("name").uppercase() }
  - see you in ${! meta("ttl_days") } days
`

	spec := NewConfigSpec().Field(NewInterpolatedStringListField("listfield"))
	env := NewEnvironment()
	parsed, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	field, err := parsed.FieldInterpolatedStringList("listfield")
	require.NoError(t, err)

	msg := NewMessage([]byte(`{"name": "world"}`))
	msg.MetaSet("ttl_days", "3")

	var out []string
	for _, f := range field {
		str, err := f.TryString(msg)
		require.NoError(t, err)

		out = append(out, str)
	}

	require.Equal(t, []string{"hello WORLD", "see you in 3 days"}, out)
}

func TestFieldInterpolatedStringList_InvalidInterpolation(t *testing.T) {
	conf := `
listfield:
  - hello ${! json("name")$$uppercas }
  - see you in ${! meta("ttl_days") } days
`

	spec := NewConfigSpec().Field(NewInterpolatedStringListField("listfield"))
	env := NewEnvironment()
	parsed, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	_, err = parsed.FieldInterpolatedStringList("listfield")
	require.ErrorIs(t, err, errInvalidInterpolation)
}
