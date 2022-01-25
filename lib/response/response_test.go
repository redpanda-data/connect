package response

import (
	"errors"
	"testing"
)

func TestError(t *testing.T) {
	err := errors.New("test error")
	res := NewError(err)

	if exp, act := err, res.Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", exp, act)
	}
	if res.SkipAck() {
		t.Error("Should not received skip ack on simple response")
	}
}

func TestNoack(t *testing.T) {
	res := NewNoack()

	if res.Error() == nil {
		t.Error("Should have received error on noack response")
	}
	if res.SkipAck() {
		t.Error("Should not have received skip ack on noack response")
	}
}

func TestAck(t *testing.T) {
	res := NewAck()

	if res.Error() != nil {
		t.Error(res.Error())
	}
	if res.SkipAck() {
		t.Error("Should not have received skip ack on ack response")
	}
}
