package cuegen

import (
	"fmt"
	"strings"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

func doComponentSpec(cs docs.ComponentSpec) (*ast.Field, error) {
	cfg := cs.Config
	if len(cfg.Children) == 0 {
		simple, err := doFieldSpec(cfg)
		if err != nil {
			return nil, err
		}
		field := &ast.Field{
			Label: ast.NewIdent(cs.Name),
			Value: simple.Value,
		}
		if cs.Summary != "" {
			ast.AddComment(field, doComment(cs.Summary))
		}
		return field, nil
	}

	fields, err := doFieldSpecs(cfg.Children)
	if err != nil {
		return nil, err
	}

	field := &ast.Field{
		Label: ast.NewIdent(cs.Name),
		Value: ast.NewStruct(fields...),
	}

	if cs.Summary != "" {
		ast.AddComment(field, doComment(cs.Summary))
	}

	return field, nil
}

func doComment(comment string) *ast.CommentGroup {
	comments := []*ast.Comment{}

	for _, v := range strings.Split(strings.TrimSpace(comment), "\n") {
		comments = append(comments, &ast.Comment{
			Slash: token.NoPos,
			Text:  "// " + v,
		})
	}

	return &ast.CommentGroup{
		Position: 0,
		List:     comments,
	}
}

func doFieldSpecs(s docs.FieldSpecs) ([]any, error) {
	var fields []any
	for _, fieldSpec := range s {
		field, err := doFieldSpec(fieldSpec)
		if err != nil {
			return nil, err
		}

		if fieldSpec.Description != "" {
			ast.AddComment(field.Label, doComment(fieldSpec.Description))
		}

		if fieldSpec.CheckRequired() {
			fields = append(fields, field)
		} else {
			fields = append(fields, field.Label, token.OPTION, field.Value)
		}
	}

	return fields, nil
}

func doFieldSpec(spec docs.FieldSpec) (*ast.Field, error) {
	switch spec.Kind {
	case "":
		return doScalarField(spec)
	case docs.KindScalar:
		return doScalarField(spec)
	case docs.KindArray:
		f, err := doScalarField(spec)
		if err != nil {
			return nil, err
		}
		f.Value = ast.NewList(&ast.Ellipsis{Type: f.Value})
		return f, nil
	case docs.Kind2DArray:
		f, err := doScalarField(spec)
		if err != nil {
			return nil, err
		}
		f.Value = ast.NewList(ast.NewList(&ast.Ellipsis{Type: f.Value}))
		return f, nil
	case docs.KindMap:
		f, err := doScalarField(spec)
		if err != nil {
			return nil, err
		}
		f.Value = ast.NewStruct(&ast.Field{
			Label: ast.NewList(ast.NewIdent("string")),
			Value: f.Value,
		})
		return f, nil
	default:
		return nil, fmt.Errorf("unrecognised field kind: %s", spec.Kind)
	}
}

func doScalarField(spec docs.FieldSpec) (*ast.Field, error) {
	label := ast.NewIdent(spec.Name)
	optionalMark := token.Blank.Pos()

	var optional token.Pos
	var val ast.Expr

	switch spec.Type {
	case docs.FieldTypeString:
		val = ast.NewIdent("string")
	case docs.FieldTypeInt:
		val = ast.NewIdent("int")
	case docs.FieldTypeFloat:
		val = ast.NewIdent("float")
	case docs.FieldTypeBool:
		val = ast.NewIdent("bool")
	case docs.FieldTypeObject:
		fields := make([]any, 0, len(spec.Children))
		for _, child := range spec.Children {
			field, err := doFieldSpec(child)
			if err != nil {
				return nil, fmt.Errorf("failed to generate type for object field: %w", err)
			}

			if child.Description != "" {
				ast.AddComment(field.Label, doComment(child.Description))
			}

			if child.CheckRequired() {
				fields = append(fields, field)
			} else {
				fields = append(fields, field.Label, token.OPTION, field.Value)
			}
		}
		val = ast.NewStruct(fields...)
	case docs.FieldTypeUnknown:
		val = ast.NewIdent("_")
	case docs.FieldTypeInput:
		// The following set of cases have unresolvable structure cycles.
		// We need to mark them as optional to break the cycle...
		// https://cuelang.org/docs/references/spec/#structural-cycles
		val, optional = identInputDisjunction, optionalMark
	case docs.FieldTypeBuffer:
		val, optional = identBufferDisjunction, optionalMark
	case docs.FieldTypeCache:
		val, optional = identCacheDisjunction, optionalMark
	case docs.FieldTypeProcessor:
		val, optional = identProcessorDisjunction, optionalMark
	case docs.FieldTypeRateLimit:
		val, optional = identRateLimitDisjunction, optionalMark
	case docs.FieldTypeOutput:
		val, optional = identOutputDisjunction, optionalMark
	case docs.FieldTypeMetrics:
		val, optional = identMetricDisjunction, optionalMark
	case docs.FieldTypeTracer:
		val, optional = identTracerDisjunction, optionalMark
	default:
		return nil, fmt.Errorf("unrecognised field type: %s", spec.Type)
	}

	return &ast.Field{
		Label:    label,
		Value:    val,
		Optional: optional,
	}, nil
}

func interpolateIdent(ident *ast.Ident) ast.Label {
	return &ast.Interpolation{Elts: []ast.Expr{
		ast.NewLit(token.STRING, "("),
		ident,
		ast.NewLit(token.STRING, ")"),
	}}
}
