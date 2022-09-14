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
			local.MetaSet("foo", "bar")
			local.MetaSet("bar", "baz")
			_ = local.MetaIter(func(k, v string) error {
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
	source.MetaSet("foo", "foo1")
	source.MetaSet("bar", "bar1")
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
			local.MetaSet("foo", "foo2")

			v, exists := local.MetaGet("foo")
			assert.True(t, exists)
			assert.Equal(t, "foo2", v)

			v, exists = local.MetaGet("bar")
			assert.True(t, exists)
			assert.Equal(t, "bar1", v)

			_ = local.MetaIter(func(k, v string) error {
				return nil
			})
			local.MetaDelete("foo")

			_, exists = local.MetaGet("foo")
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
