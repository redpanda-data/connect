package metrics

import (
	"testing"
)

func TestStatsDTags(t *testing.T) {
	tagslice := tags([]string{"tag1", "tag2"}, []string{"value1", "value2"})
	if "tag1:value1" != tagslice[0] {
		t.Errorf("%s != %s", "tag1:value1", tagslice[0])
	}
	if "tag2:value2" != tagslice[1] {
		t.Errorf("%s != %s", "tag2:value2", tagslice[1])
	}

	tagslice = tags([]string{"tag1", "tag2"}, []string{"value1"})
	if "tag1:value1" != tagslice[0] {
		t.Errorf("%s != %s", "tag1:value1", tagslice[0])
	}

}
