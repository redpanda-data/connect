package cue

import (
	"cuelang.org/go/cue/errors"
	"go.uber.org/multierr"
)

// cueErrorsToMultiErr unboxes a possible group of CUE errors and repackages
// them with multierr to propogate upward. The CUE error type only prints the
// last error and obscures the rest which is not great for debugging.
func cueErrorsToMultiErr(err error) error {
	errs := errors.Errors(err)

	var out error
	for _, e := range errs {
		out = multierr.Append(out, e)
	}

	return out
}
