package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// LineAndColOf returns the line and column position of a tailing clip from an
// input.
func LineAndColOf(input, clip []rune) (line, col int) {
	char := len(input) - len(clip)

	lines := strings.Split(string(input), "\n")
	for ; line < len(lines); line++ {
		if char < (len(lines[line]) + 1) {
			break
		}
		char = char - len(lines[line]) - 1
	}

	return line + 1, char + 1
}

// Error represents an error that has occurred whilst attempting to apply a
// parser function to a given input. A slice of abstract names should be
// provided outlining tokens or characters that were expected and not found at
// the input in order to provide a useful error message.
//
// The input at the point of the error can be used in order to infer where
// exactly in the input the error occurred with len(input) - len(err.Input).
type Error struct {
	Input    []rune
	Err      error
	Expected []string
}

// NewError creates a parser error from the input and a list of expected tokens.
// This is a passive error indicating that this particular parser did not
// succeed, but that other parsers should be tried if applicable.
func NewError(input []rune, expected ...string) *Error {
	return &Error{
		Input:    input,
		Expected: expected,
	}
}

// NewFatalError creates a parser error from the input and a wrapped fatal error
// indicating that this parse succeeded partially, but a requirement was not met
// that means the parsed input is invalid and that all parsing should stop.
func NewFatalError(input []rune, err error, expected ...string) *Error {
	return &Error{
		Input:    input,
		Err:      err,
		Expected: expected,
	}
}

func (e *Error) errorMsg(includeGot bool) string {
	inputSnippet := string(e.Input)
	if len(e.Input) > 5 {
		inputSnippet = string(e.Input[:5])
	}

	var msg string
	if e.Err != nil {
		msg = e.Err.Error()
	}

	if len(e.Expected) == 0 {
		if msg == "" {
			if inputSnippet == "" {
				msg = "encountered unexpected end of input"
			} else {
				msg = "encountered unexpected input"
			}
		}
		if includeGot {
			if inputSnippet != "" {
				msg += fmt.Sprintf(": %v", inputSnippet)
			}
		}
		return msg
	}

	if msg != "" {
		msg += ": "
	}

	if len(e.Expected) == 1 {
		msg += fmt.Sprintf("expected %v", e.Expected[0])
	} else {
		dedupeMap := make(map[string]struct{}, len(e.Expected))
		expected := make([]string, 0, len(e.Expected))
		for _, exp := range e.Expected {
			if _, exists := dedupeMap[exp]; !exists {
				expected = append(expected, exp)
			}
			dedupeMap[exp] = struct{}{}
		}

		var buf bytes.Buffer
		buf.WriteString("expected ")
		for i, exp := range expected {
			if i == len(expected)-1 {
				if len(expected) > 2 {
					buf.WriteByte(',')
				}
				buf.WriteString(" or ")
			} else if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(exp)
		}
		msg += buf.String()
	}

	if includeGot {
		if inputSnippet != "" {
			msg += fmt.Sprintf(", got: %v", inputSnippet)
		} else {
			msg += ", but reached end of input"
		}
	}
	return msg
}

// Error returns a human readable error string.
func (e *Error) Error() string {
	return e.errorMsg(true)
}

// Unwrap returns the underlying fatal error (or nil).
func (e *Error) Unwrap() error {
	return e.Err
}

// IsFatal returns whether this parser error should be considered fatal, and
// therefore sibling parser candidates should not be tried.
func (e *Error) IsFatal() bool {
	return e.Err != nil
}

// Add context from another error into this one.
func (e *Error) Add(from *Error) {
	e.Expected = append(e.Expected, from.Expected...)
	if e.Err == nil {
		e.Err = from.Err
	}
}

// ErrorAtPosition returns a human readable error string including the line and
// character position of the error.
func (e *Error) ErrorAtPosition(input []rune) string {
	importErr, isImport := e.Err.(*ImportError)

	var errStr string
	if isImport {
		errStr = fmt.Sprintf(
			"failed to parse import '%v': %v", importErr.filepath,
			importErr.perr.ErrorAtPosition(importErr.content),
		)
	} else {
		errStr = e.errorMsg(false)
	}

	line, char := LineAndColOf(input, e.Input)
	return fmt.Sprintf("line %v char %v: %v", line, char, errStr)
}

// ErrorAtChar returns a human readable error string including the character
// position of the error.
func (e *Error) ErrorAtChar(input []rune) string {
	importErr, isImport := e.Err.(*ImportError)

	var errStr string
	if isImport {
		errStr = fmt.Sprintf(
			"failed to parse import '%v': %v", importErr.filepath,
			importErr.perr.ErrorAtChar(importErr.perr.Input),
		)
	} else {
		errStr = e.errorMsg(false)
	}

	char := len(input) - len(e.Input)
	return fmt.Sprintf("char %v: %v", char+1, errStr)
}

// ErrorAtPositionStructured returns a human readable error string including the
// line and character position of the error formatted in a more structured way,
// this message isn't appropriate to write within structured logs as the
// formatting will be broken.
func (e *Error) ErrorAtPositionStructured(filepath string, input []rune) string {
	importErr, isImport := e.Err.(*ImportError)

	var errStr string
	if isImport {
		errStr = fmt.Sprintf("failed to parse import '%v'", importErr.filepath)
	} else {
		errStr = e.errorMsg(false)
	}

	line, char := 0, len(input)-len(e.Input)
	var contextLine string

	lines := strings.Split(string(input), "\n")
	for ; line < len(lines); line++ {
		if char < (len(lines[line]) + 1) {
			maxLen := len(lines[line])
			if char < (maxLen - 60) {
				maxLen = char + 60
			}
			contextLine = lines[line][:maxLen]
			break
		}
		char = char - len(lines[line]) - 1
	}

	lineStr := strconv.FormatInt(int64(line+1), 10)
	linePadding := strings.Repeat(" ", len(lineStr))

	filepathStr := ""
	if filepath != "" {
		filepathStr = filepath + ": "
	}

	structuredMsg := fmt.Sprintf(`%vline %v char %v: %v
%v |
%v | %v
%v | %v^---`,
		filepathStr,
		lineStr, char+1, errStr,
		linePadding,
		lineStr, contextLine,
		linePadding, strings.Repeat(" ", char))

	if isImport {
		structuredMsg = structuredMsg + "\n\n" + importErr.perr.ErrorAtPositionStructured(importErr.filepath, importErr.content)
	}

	return structuredMsg
}

// ImportError wraps a parser error with an import file path. When a fatal error
// wraps.
type ImportError struct {
	filepath string
	content  []rune
	perr     *Error
}

// NewImportError wraps a parser error with a filepath for when a parser has
// attempted to parse content of an imported file.
func NewImportError(path string, input []rune, err *Error) *ImportError {
	return &ImportError{
		filepath: path,
		content:  input,
		perr:     err,
	}
}

// Error implements the standard error interface.
func (i *ImportError) Error() string {
	return i.perr.Error()
}
