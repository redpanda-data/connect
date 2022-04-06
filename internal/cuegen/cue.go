package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func doComponent(cs docs.ComponentSpec) (*ast.Field, error) {
	cfg := cs.Config
	if len(cfg.Children) == 0 {
		simple, err := doFieldSpec(cfg)
		if err != nil {
			return nil, err
		}
		return &ast.Field{
			Label: ast.NewIdent(cs.Name),
			Value: simple.Value,
		}, nil
	}

	fields, err := doFieldSpecs(cfg.Children)
	if err != nil {
		return nil, err
	}

	return &ast.Field{
		Label: ast.NewIdent(cs.Name),
		Value: ast.NewStruct(fields...),
	}, nil
}

func doFieldSpecs(s docs.FieldSpecs) ([]interface{}, error) {
	var fields []interface{}
	for _, fieldSpec := range s {
		field, err := doFieldSpec(fieldSpec)
		if err != nil {
			return nil, err
		}
		if checkRequired(fieldSpec) {
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
		fields := make([]interface{}, 0, len(spec.Children))
		for _, child := range spec.Children {
			field, err := doFieldSpec(child)
			if err != nil {
				return nil, fmt.Errorf("failed to generate type for object field: %w", err)
			}

			if checkRequired(child) {
				fields = append(fields, field)
			} else {
				fields = append(fields, field.Label, token.OPTION, field.Value)
			}
		}
		val = ast.NewStruct(fields...)
	case docs.FieldTypeUnknown:
		val = ast.NewIdent("_")
	case docs.FieldTypeInput:
		// The following set of case switches have unresolvable structure cycles.
		// We need to form a disjunction with `null`` to break the cycle...
		// https://cuelang.org/docs/references/spec/#structural-cycles
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identInputDisjunction)
	case docs.FieldTypeBuffer:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identBufferDisjunction)
	case docs.FieldTypeCache:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identCacheDisjunction)
	case docs.FieldTypeProcessor:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identProcessorDisjunction)
	case docs.FieldTypeRateLimit:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identRateLimitDisjunction)
	case docs.FieldTypeOutput:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identOutputDisjunction)
	case docs.FieldTypeMetrics:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identMetricDisjunction)
	case docs.FieldTypeTracer:
		val = ast.NewBinExpr(token.OR, ast.NewIdent("null"), identTracerDisjunction)
	default:
		return nil, fmt.Errorf("unrecognised field type: %s", spec.Type)
	}

	return &ast.Field{
		Label: ast.NewIdent(spec.Name),
		Value: val,
	}, nil
}

func interpolateIdent(ident *ast.Ident) ast.Label {
	return &ast.Interpolation{Elts: []ast.Expr{
		ast.NewLit(token.STRING, "("),
		ident,
		ast.NewLit(token.STRING, ")"),
	}}
}

// TODO: Replace with FieldSpec.CheckRequired when available
func checkRequired(f docs.FieldSpec) bool {
	if f.IsOptional {
		return false
	}
	if f.Default != nil {
		return false
	}
	if len(f.Children) == 0 {
		return true
	}

	// If none of the children are required then this field is not required.
	for _, child := range f.Children {
		if checkRequired(child) {
			return true
		}
	}
	return false
}
