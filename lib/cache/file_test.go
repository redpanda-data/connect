package cache

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestFileCache(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeFile
	dir, err := ioutil.TempDir("", "benthos_file_cache_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	conf.File.Directory = dir

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	expErr := types.ErrKeyNotFound
	if _, act := c.Get("foo"); act != expErr {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set("foo", []byte("1")); err != nil {
		t.Error(err)
	}

	exp := "1"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Add("bar", []byte("2")); err != nil {
		t.Error(err)
	}

	exp = "2"
	if act, err := c.Get("bar"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	expErr = types.ErrKeyAlreadyExists
	if act := c.Add("foo", []byte("2")); expErr != act {
		t.Errorf("Wrong error returned: %v != %v", act, expErr)
	}

	if err = c.Set("foo", []byte("3")); err != nil {
		t.Error(err)
	}

	exp = "3"
	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong result: %v != %v", string(act), exp)
	}

	if err = c.Delete("foo"); err != nil {
		t.Error(err)
	}

	if _, err = c.Get("foo"); err != types.ErrKeyNotFound {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyNotFound)
	}
}

//------------------------------------------------------------------------------
