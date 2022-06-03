package message

import (
	"errors"
)

// Errors returned by the message type.
var (
	ErrMessagePartNotExist = errors.New("target message part does not exist")
	ErrBadMessageBytes     = errors.New("serialised message bytes were in unexpected format")
)
