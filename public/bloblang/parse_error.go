package bloblang

import "github.com/Jeffail/benthos/v3/internal/bloblang/parser"

// ParseError is a structured error type for Bloblang parser errors that
// provides access to information such as the line and column where the error
// occurred.
type ParseError struct {
	Line   int
	Column int

	input []rune
	iErr  *parser.Error
}

// Error returns a single line error string.
func (p *ParseError) Error() string {
	return p.iErr.Error()
}

// ErrorMultiline returns an error string spanning multiple lines that provides
// a cleaner view of the specific error.
func (p *ParseError) ErrorMultiline() string {
	return p.iErr.ErrorAtPositionStructured("", p.input)
}

func internalToPublicParserError(input []rune, p *parser.Error) *ParseError {
	pErr := &ParseError{
		input: input,
		iErr:  p,
	}
	pErr.Line, pErr.Column = parser.LineAndColOf([]rune(input), p.Input)
	return pErr
}
