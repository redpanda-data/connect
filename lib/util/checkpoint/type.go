package checkpoint

import (
	"errors"
)

// ErrOutOfSync is returned when an offset to be tracked is less than or equal
// to the current highest tracked offset.
var ErrOutOfSync = errors.New("provided offset was out of sync")

// ErrResolvedOffsetNotTracked is returned when an offset to be resolved has not
// been tracked (or was already resolved).
var ErrResolvedOffsetNotTracked = errors.New("resolved offset was not tracked")

// Type receives an ordered feed of integer based offsets being tracked, and an
// unordered feed of integer based offsets that are resolved, and is able to
// return the highest offset currently able to be committed such that an
// unresolved offset is never committed.
type Type struct {
	base          int
	maxResolved   int
	maxUnresolved int

	offset  int
	pending []int
	buf     []int
}

// New returns a new checkpointer from a base offset, which is the value
// returned when no checkpoints have yet been resolved. The base can be zero.
func New(base int) *Type {
	buffer := make([]int, 100)
	return &Type{
		base:    base,
		pending: buffer[:0],
		buf:     buffer,
	}
}

// Highest returns the current highest checkpoint.
func (t *Type) Highest() int {
	return t.base + t.maxResolved
}

// Track a new unresolved integer offset. This offset will be cached until it is
// marked as resolved. While it is cached no higher valued offset will ever be
// committed. If the provided value is lower than an already provided value an
// error is returned.
func (t *Type) Track(i int) error {
	if t.offset == cap(t.buf) {
		if len(t.pending) == cap(t.buf) {
			// Reached the limit of our buffer, create a new one.
			t.buf = make([]int, cap(t.buf)*2)
		}
		copy(t.buf, t.pending)
		t.pending = t.buf[0:len(t.pending)]
		t.offset = len(t.pending)
	}
	if len(t.pending) > 0 && t.pending[len(t.pending)-1] >= i {
		return ErrOutOfSync
	}
	t.offset++
	t.pending = t.pending[:len(t.pending)+1]
	t.pending[len(t.pending)-1] = i
	return nil
}

// MustTrack is the same as Track but panics instead of returning an error.
func (t *Type) MustTrack(i int) {
	if err := t.Track(i); err != nil {
		panic(err)
	}
}

// Resolve a tracked offset by allowing it to be committed. The highest possible
// offset to be committed is returned, or an error if the provided offset was
// not recognised.
func (t *Type) Resolve(offset int) (int, error) {
	var i, v int
	for i, v = range t.pending {
		if v >= offset {
			break
		}
	}
	if v != offset {
		return 0, ErrResolvedOffsetNotTracked
	}
	if v > t.maxUnresolved {
		t.maxUnresolved = v
	}
	if i > 0 {
		// The offset wasn't the next checkpoint.
		v = t.maxResolved
		copy(t.pending[1:], t.pending[:i])
	} else if len(t.pending) == 1 {
		t.maxResolved = t.maxUnresolved
	} else {
		t.maxResolved = v
	}
	t.pending = t.pending[1:]
	return t.base + t.maxResolved, nil
}

// MustResolve is the same as Resolve but panics instead of returning an error.
func (t *Type) MustResolve(offset int) int {
	i, err := t.Resolve(offset)
	if err != nil {
		panic(err)
	}
	return i
}
