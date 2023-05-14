package service

// NewOutputMaxInFlightField creates a common field for determining the maximum
// number of in-flight messages an output should allow. This function is a
// short-hand way of creating an integer field with the common name
// max_in_flight, with a typical default of 64.
func NewOutputMaxInFlightField() *ConfigField {
	return NewIntField("max_in_flight").
		Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
		Default(64)
}

// FieldMaxInFlight accesses a field from a parsed config that was defined
// either with NewInputMaxInFlightField or NewOutputMaxInFlightField, and
// returns either an integer or an error if the value was invalid.
func (p *ParsedConfig) FieldMaxInFlight() (int, error) {
	return p.FieldInt("max_in_flight")
}
