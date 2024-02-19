package message

import (
	"sync"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrentMutationsFromNil(t *testing.T) {
	source := newMessageBytes(nil)
	kickOffChan := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-kickOffChan

			local := source.ShallowCopy()
			local.MetaSetMut("foo", "bar")
			local.MetaSetMut("bar", "baz")
			_ = local.MetaIterMut(func(k string, v any) error {
				return nil
			})
			local.MetaDelete("foo")

			local.SetBytes([]byte(`new thing`))
			local.SetStructuredMut(map[string]any{
				"foo": "bar",
			})

			vThing, err := local.AsStructuredMut()
			require.NoError(t, err)

			_, err = gabs.Wrap(vThing).Set("baz", "foo")
			require.NoError(t, err)

			vBytes := local.AsBytes()
			assert.Equal(t, `{"foo":"baz"}`, string(vBytes))
		}()
	}

	close(kickOffChan)
	wg.Wait()
}

func TestConcurrentMutationsFromStructured(t *testing.T) {
	source := newMessageBytes(nil)
	source.MetaSetMut("foo", "foo1")
	source.MetaSetMut("bar", "bar1")
	source.SetStructuredMut(map[string]any{
		"foo": "bar",
	})

	kickOffChan := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-kickOffChan

			local := source.ShallowCopy()
			local.MetaSetMut("foo", "foo2")

			v, exists := local.MetaGetMut("foo")
			assert.True(t, exists)
			assert.Equal(t, "foo2", v)

			v, exists = local.MetaGetMut("bar")
			assert.True(t, exists)
			assert.Equal(t, "bar1", v)

			_ = local.MetaIterMut(func(k string, v any) error {
				return nil
			})
			local.MetaDelete("foo")

			_, exists = local.MetaGetMut("foo")
			assert.False(t, exists)

			vThing, err := local.AsStructuredMut()
			require.NoError(t, err)

			_, err = gabs.Wrap(vThing).Set("baz", "foo")
			require.NoError(t, err)

			vBytes := local.AsBytes()
			assert.Equal(t, `{"foo":"baz"}`, string(vBytes))
		}()
	}

	close(kickOffChan)
	wg.Wait()
}

func TestSetNil(t *testing.T) {
	source := newMessageBytes(nil)
	source.SetStructured(map[string]any{
		"foo": "bar",
	})

	v, err := source.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, v)

	source.SetStructured(nil)

	v, err = source.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, nil, v)
}
