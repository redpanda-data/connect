package service

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// LintType is a discrete linting type.
// NOTE: These should be kept in sync with ./internal/docs/field.go:586.
type LintType int

const (
	// LintCustom means a custom linting rule failed.
	LintCustom LintType = iota

	// LintFailedRead means a configuration could not be read.
	LintFailedRead LintType = iota

	// LintInvalidOption means the field value was not one of the explicit list
	// of options.
	LintInvalidOption LintType = iota

	// LintBadLabel means the label contains invalid characters.
	LintBadLabel LintType = iota

	// LintMissingLabel means the label is missing when required.
	LintMissingLabel LintType = iota

	// LintDuplicateLabel means the label collides with another label.
	LintDuplicateLabel LintType = iota

	// LintBadBloblang means the field contains invalid Bloblang.
	LintBadBloblang LintType = iota

	// LintShouldOmit means the field should be omitted.
	LintShouldOmit LintType = iota

	// LintComponentMissing means a component value was expected but the type is
	// missing.
	LintComponentMissing LintType = iota

	// LintComponentNotFound means the specified component value is not
	// recognised.
	LintComponentNotFound LintType = iota

	// LintUnknown means the field is unknown.
	LintUnknown LintType = iota

	// LintMissing means a field was required but missing.
	LintMissing LintType = iota

	// LintExpectedArray means an array value was expected but something else
	// was provided.
	LintExpectedArray LintType = iota

	// LintExpectedObject means an object value was expected but something else
	// was provided.
	LintExpectedObject LintType = iota

	// LintExpectedScalar means a scalar value was expected but something else
	// was provided.
	LintExpectedScalar LintType = iota

	// LintDeprecated means a field is deprecated and should not be used.
	LintDeprecated LintType = iota
)

func convertDocsLintType(d docs.LintType) LintType {
	switch d {
	case docs.LintCustom:
		return LintCustom
	case docs.LintFailedRead:
		return LintFailedRead
	case docs.LintInvalidOption:
		return LintInvalidOption
	case docs.LintBadLabel:
		return LintBadLabel
	case docs.LintMissingLabel:
		return LintMissingLabel
	case docs.LintDuplicateLabel:
		return LintDuplicateLabel
	case docs.LintBadBloblang:
		return LintBadBloblang
	case docs.LintShouldOmit:
		return LintShouldOmit
	case docs.LintComponentMissing:
		return LintComponentMissing
	case docs.LintComponentNotFound:
		return LintComponentNotFound
	case docs.LintUnknown:
		return LintUnknown
	case docs.LintMissing:
		return LintMissing
	case docs.LintExpectedArray:
		return LintExpectedArray
	case docs.LintExpectedObject:
		return LintExpectedObject
	case docs.LintExpectedScalar:
		return LintExpectedScalar
	case docs.LintDeprecated:
		return LintDeprecated
	}
	return LintCustom
}

// Lint represents a configuration file linting error.
type Lint struct {
	Line   int
	Column int
	Type   LintType
	What   string
}

// Error returns an error string.
func (l Lint) Error() string {
	return fmt.Sprintf("(%v,%v) %v", l.Line, l.Column, l.What)
}

// LintError is an error type that represents one or more configuration file
// linting errors that were encountered.
type LintError []Lint

// Error returns an error string.
func (e LintError) Error() string {
	var lintsCollapsed bytes.Buffer
	for i, l := range e {
		if i > 0 {
			lintsCollapsed.WriteString("\n")
		}
		fmt.Fprint(&lintsCollapsed, l.Error())
	}
	return fmt.Sprintf("lint errors: %v", lintsCollapsed.String())
}

func convertDocsLint(l docs.Lint) Lint {
	return Lint{
		Line:   l.Line,
		Column: l.Column,
		Type:   convertDocsLintType(l.Type),
		What:   l.What,
	}
}

func lintsToErr(lints []docs.Lint) error {
	if len(lints) == 0 {
		return nil
	}
	var e LintError
	for _, l := range lints {
		e = append(e, convertDocsLint(l))
	}
	return e
}

func convertDocsLintErr(err error) error {
	var l docs.Lint
	if errors.As(err, &l) {
		return convertDocsLint(l)
	}
	return err
}
