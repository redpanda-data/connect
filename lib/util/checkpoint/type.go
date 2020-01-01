package checkpoint

import "errors"

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
	base    int
	edge    int
	pending []int

	highestBlocked  int
	highestResolved int
}

// New returns a new checkpointer from a base offset, which is the value
// returned when no checkpoints have yet been resolved. The base can be zero.
func New(base int) *Type {
	return &Type{
		highestResolved: base,
	}
}

// Highest returns the current highest checkpoint.
func (t *Type) Highest() int {
	return t.highestResolved
}

// Track a new unresolved integer offset. This offset will be cached until it is
// marked as resolved. While it is cached no higher valued offset will ever be
// committed. If the provided value is lower than an already provided value an
// error is returned.
func (t *Type) Track(i int) error {
	if t.edge >= i {
		return ErrOutOfSync
	}
	if len(t.pending) == 0 && t.base == 0 {
		t.base = i
	} else {
		t.pending = append(t.pending, i-t.edge)
	}
	t.edge = i
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
	// If our resolved offset is also the base then it can be immediately
	// resolved as there are no pending lower offsets.
	if offset == t.base {
		t.highestResolved = offset
		if len(t.pending) == 0 {
			t.base = 0
			t.edge = 0
			if t.highestBlocked > t.highestResolved {
				t.highestResolved = t.highestBlocked
			}
		} else {
			t.base = t.base + t.pending[0]
			for i := 0; i < len(t.pending)-1; i++ {
				t.pending[i] = t.pending[i+1]
			}
			t.pending = t.pending[:len(t.pending)-1]
		}
		return t.highestResolved, nil
	}

	// Otherwise we cannot commit this resolved offset and must only remove it
	// from our pending list.

	// Best case scenario, our resolved offset is our highest pending offset, so
	// we can simply remove it from the edge.
	if offset == t.edge {
		t.edge = t.edge - t.pending[len(t.pending)]

		// Second best case scenario, all pending offsets are exactly 1 larger
		// than the previous, so we can avoid iterating all values to find our
		// resolving offset.
	} else if t.edge == t.base+len(t.pending) {
		t.edge = t.edge - 1
		t.pending = t.pending[:len(t.pending)-1]

		// Worst case scenario as we're forced to iterate the pending slice
		// until we reach our target offset. Then we must remove it and adjust
		// all following values.
	} else {
		currentPendingValue := t.base
		var i int
		for ; i < len(t.pending)-1; i++ {
			currentPendingValue += t.pending[i]
			if offset == currentPendingValue {
				// We found the element representing our offset, replace it with
				// the next value.
				t.pending[i] = t.pending[i+1] + t.pending[i]
				break
			}
		}
		if offset == currentPendingValue {
			return 0, ErrResolvedOffsetNotTracked
		}
		for i++; i < len(t.pending)-1; i++ {
			t.pending[i] = t.pending[i+1]
		}
		t.pending = t.pending[:len(t.pending)-1]
	}

	if t.highestBlocked < offset {
		t.highestBlocked = offset
	}
	return t.highestResolved, nil
}

// MustResolve is the same as Resolve but panics instead of returning an error.
func (t *Type) MustResolve(offset int) int {
	i, err := t.Resolve(offset)
	if err != nil {
		panic(err)
	}
	return i
}
