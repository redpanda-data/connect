package output

// GetMaxInFlight attempts to derive a max in flight value from the provided
// output, and returns the count and a boolean flag indicating whether the
// output provided that value. This value can be used to determine a sensible
// value for parent outputs, but should not be relied upon as part of dispatcher
// logic.
func GetMaxInFlight(o interface{}) (int, bool) {
	if mf, ok := o.(interface {
		MaxInFlight() (int, bool)
	}); ok {
		return mf.MaxInFlight()
	}
	return 0, false
}
