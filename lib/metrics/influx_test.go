package metrics

import "testing"

func TestInfluxInterface(t *testing.T) {
	o := &Influx{}
	if Type(o) == nil {
		t.Errorf("Type does not satisfy Type interface.")
	}
}
