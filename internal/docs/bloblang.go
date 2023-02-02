package docs

import (
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// LintBloblangMapping is function for linting a config field expected to be a
// bloblang mapping.
func LintBloblangMapping(ctx LintContext, line, col int, v any) []Lint {
	str, ok := v.(string)
	if !ok {
		return nil
	}
	if str == "" {
		return nil
	}
	_, err := ctx.BloblangEnv.Parse(str)
	if err == nil {
		return nil
	}
	if mErr, ok := err.(*bloblang.ParseError); ok {
		lint := NewLintError(line+mErr.Line-1, LintBadBloblang, mErr.ErrorMultiline())
		lint.Column = col + mErr.Column
		return []Lint{lint}
	}
	return []Lint{NewLintError(line, LintBadBloblang, err.Error())}
}

// LintBloblangField is function for linting a config field expected to be an
// interpolation string.
func LintBloblangField(ctx LintContext, line, col int, v any) []Lint {
	str, ok := v.(string)
	if !ok {
		return nil
	}
	if str == "" {
		return nil
	}
	err := ctx.BloblangEnv.CheckInterpolatedString(str)
	if err == nil {
		return nil
	}
	if mErr, ok := err.(*bloblang.ParseError); ok {
		lint := NewLintError(line+mErr.Line-1, LintBadBloblang, mErr.ErrorMultiline())
		lint.Column = col + mErr.Column
		return []Lint{lint}
	}
	return []Lint{NewLintError(line, LintBadBloblang, err.Error())}
}
