package interop

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
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

// Connect does nothing, use Unwrap instead.
func (u *UnwrapInternalInput) Connect(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

// ReadBatch does nothing, use Unwrap instead.
func (u *UnwrapInternalInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, component.ErrNotUnwrapped
}

// Close does nothing, use Unwrap instead.
func (u *UnwrapInternalInput) Close(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

//------------------------------------------------------------------------------

// UnwrapInternalBatchProcessor is a no-op implementation of an internal
// component that allows a public/service environment to unwrap it straight into
// the needed format during construction. This is useful in cases where we're
// migrating internal components to use the public configuration APIs but aren't
// quite ready to move the full implementation yet.
type UnwrapInternalBatchProcessor struct {
	s processor.V1
}

// NewUnwrapInternalBatchProcessor returns wraps an internal component
// implementation.
func NewUnwrapInternalBatchProcessor(s processor.V1) *UnwrapInternalBatchProcessor {
	return &UnwrapInternalBatchProcessor{s: s}
}

// Unwrap in order to obtain the underlying Streamed implementation.
func (u *UnwrapInternalBatchProcessor) Unwrap() processor.V1 {
	return u.s
}

// ProcessBatch does nothing, use Unwrap instead.
func (u *UnwrapInternalBatchProcessor) ProcessBatch(ctx context.Context, b service.MessageBatch) ([]service.MessageBatch, error) {
	return nil, component.ErrNotUnwrapped
}

// Close does nothing, use Unwrap instead.
func (u *UnwrapInternalBatchProcessor) Close(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

//------------------------------------------------------------------------------

// UnwrapInternalOutput is a no-op implementation of an internal component that
// allows a public/service environment to unwrap it straight into the needed
// format during construction. This is useful in cases where we're migrating
// internal components to use the public configuration APIs but aren't quite
// ready to move the full implementation yet.
type UnwrapInternalOutput struct {
	s output.Streamed
}

// NewUnwrapInternalOutput returns wraps an internal component implementation.
func NewUnwrapInternalOutput(s output.Streamed) *UnwrapInternalOutput {
	return &UnwrapInternalOutput{s: s}
}

// Unwrap in order to obtain the underlying Streamed implementation.
func (u *UnwrapInternalOutput) Unwrap() output.Streamed {
	return u.s
}

// Connect does nothing, use Unwrap instead.
func (u *UnwrapInternalOutput) Connect(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

// WriteBatch does nothing, use Unwrap instead.
func (u *UnwrapInternalOutput) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	return component.ErrNotUnwrapped
}

// Close does nothing, use Unwrap instead.
func (u *UnwrapInternalOutput) Close(ctx context.Context) error {
	return component.ErrNotUnwrapped
}

//------------------------------------------------------------------------------

// UnwrapManagement a public *service.Resources type into an internal
// bundle.NewManagement type. This solution will eventually be phased out as it
// is only used for migrating components.
func UnwrapManagement(r *service.Resources) bundle.NewManagement {
	return r.XUnwrapper().(interface {
		Unwrap() bundle.NewManagement
	}).Unwrap()
}
