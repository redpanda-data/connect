package interop

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/public/service"
)

// UnwrapOwnedInput attempts to unwrap a public owned component into an internal
// variant. This is useful in cases where we're migrating internal components to
// use the public configuration APIs but aren't quite ready to move the full
// implementation yet.
func UnwrapOwnedInput(o *service.OwnedInput) input.Streamed {
	return o.XUnwrapper().(interface {
		Unwrap() input.Streamed
	}).Unwrap()
}

// UnwrapInternalInput is a no-op implementation of an internal component that
// allows a public/service environment to unwrap it straight into the needed
// format during construction. This is useful in cases where we're migrating
// internal components to use the public configuration APIs but aren't quite
// ready to move the full implementation yet.
type UnwrapInternalInput struct {
	s input.Streamed
}

// NewUnwrapInternalInput returns wraps an internal component implementation.
func NewUnwrapInternalInput(s input.Streamed) *UnwrapInternalInput {
	return &UnwrapInternalInput{s: s}
}

// Unwrap in order to obtain the underlying Streamed implementation.
func (u *UnwrapInternalInput) Unwrap() input.Streamed {
	return u.s
}

// Connect does nothing, Unwrap the Streamed instead.
func (u *UnwrapInternalInput) Connect(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

// ReadBatch does nothing, Unwrap the Streamed instead.
func (u *UnwrapInternalInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, component.ErrNotUnwrapped
}

// Close does nothing, Unwrap the Streamed instead.
func (u *UnwrapInternalInput) Close(ctx context.Context) error {
	return component.ErrNotUnwrapped
}
