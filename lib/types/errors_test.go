package types

import "testing"

func TestHTTPError(t *testing.T) {
	err := ErrUnexpectedHTTPRes{
		Code: 0,
		S:    "test str",
	}

	exp, act := `HTTP request returned unexpected response code (0): test str`, err.Error()
	if exp != act {
		t.Errorf("Wrong Error() from ErrUnexpectedHTTPRes: %v != %v", exp, act)
	}
}
