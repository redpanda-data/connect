package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataFilterConfig(t *testing.T) {
	configSpec := NewConfigSpec().Field(NewMetadataFilterField("foo"))

	configParsed, err := configSpec.ParseYAML(`
foo:
  include_prefixes: [ foo_ ]
  include_patterns: [ "meo?w" ]
`, nil)
	require.NoError(t, err)

	_, err = configParsed.FieldMetadataFilter("bar")
	require.Error(t, err)

	f, err := configParsed.FieldMetadataFilter("foo")
	require.NoError(t, err)

	msg := NewMessage(nil)
	msg.MetaSet("foo_1", "foo value")
	msg.MetaSet("bar_1", "bar value")
	msg.MetaSet("baz_1", "baz value")
	msg.MetaSet("woof_1", "woof value")
	msg.MetaSet("meow_1", "meow value")
	msg.MetaSet("mew_1", "mew value")

	seen := map[string]string{}
	require.NoError(t, f.Walk(msg, func(key, value string) error {
		seen[key] = value
		return nil
	}))

	assert.Equal(t, map[string]string{
		"foo_1":  "foo value",
		"meow_1": "meow value",
		"mew_1":  "mew value",
	}, seen)
}
