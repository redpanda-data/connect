package text

//------------------------------------------------------------------------------

// InterpolatedString holds a string that potentially has interpolation
// functions. Each time Get is called any functions are replaced with their
// evaluated results in the string.
type InterpolatedString struct {
	str         string
	strBytes    []byte
	interpolate bool
}

// Get evaluates functions within the original string and returns the result.
func (i *InterpolatedString) Get(msg Message) string {
	return i.GetFor(msg, 0)
}

// GetFor evaluates functions within the original string and returns the result,
// evaluated for a specific message part of the message.
func (i *InterpolatedString) GetFor(msg Message, index int) string {
	if !i.interpolate {
		return i.str
	}
	return string(ReplaceFunctionVariablesFor(msg, index, i.strBytes))
}

// NewInterpolatedString returns a type that evaluates function interpolations
// on a provided string each time Get is called.
func NewInterpolatedString(str string) *InterpolatedString {
	strI := &InterpolatedString{
		str: str,
	}
	if strBytes := []byte(str); ContainsFunctionVariables(strBytes) {
		strI.strBytes = strBytes
		strI.interpolate = true
	}
	return strI
}

//------------------------------------------------------------------------------

// InterpolatedBytes holds a byte slice that potentially has interpolation
// functions. Each time Get is called any functions are replaced with their
// evaluated results in the byte slice.
type InterpolatedBytes struct {
	v           []byte
	interpolate bool
}

// Get evaluates functions within the byte slice and returns the result.
func (i *InterpolatedBytes) Get(msg Message) []byte {
	return i.GetFor(msg, 0)
}

// GetFor evaluates functions within the byte slice and returns the result,
// evaluated for a specific message part of the message.
func (i *InterpolatedBytes) GetFor(msg Message, index int) []byte {
	if !i.interpolate {
		return i.v
	}
	return ReplaceFunctionVariablesFor(msg, index, i.v)
}

// NewInterpolatedBytes returns a type that evaluates function interpolations
// on a provided byte slice each time Get is called.
func NewInterpolatedBytes(v []byte) *InterpolatedBytes {
	return &InterpolatedBytes{
		v:           v,
		interpolate: ContainsFunctionVariables(v),
	}
}

//------------------------------------------------------------------------------
