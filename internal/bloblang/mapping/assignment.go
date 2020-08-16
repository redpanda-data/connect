package mapping

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// AssignmentContext contains references to all potential assignment
// destinations of a given mapping.
type AssignmentContext struct {
	Maps  map[string]query.Function
	Vars  map[string]interface{}
	Meta  types.Metadata
	Value *interface{}
}

// Assignment represents a way of assigning a queried value to something within
// an assignment context. This could be a Benthos message, a variable, a
// metadata field, etc.
type Assignment interface {
	Apply(value interface{}, ctx AssignmentContext) error
	Target() TargetPath
}

//------------------------------------------------------------------------------

// VarAssignment creates a variable and assigns it a value.
type VarAssignment struct {
	name string
}

// NewVarAssignment creates a new variable assignment.
func NewVarAssignment(name string) *VarAssignment {
	return &VarAssignment{
		name: name,
	}
}

// Apply a value to a variable.
func (v *VarAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	if _, deleted := value.(query.Delete); deleted {
		delete(ctx.Vars, v.name)
	} else {
		ctx.Vars[v.name] = value
	}
	return nil
}

// Target returns a representation of what the assignment targets.
func (v *VarAssignment) Target() TargetPath {
	return NewTargetPath(TargetVariable, v.name)
}

//------------------------------------------------------------------------------

// MetaAssignment assigns a value to a metadata key of a message. If the key is
// omitted and the value is an object then the metadata of the message is reset
// to the contents of the value.
type MetaAssignment struct {
	key *string
}

// NewMetaAssignment creates a new meta assignment.
func NewMetaAssignment(key *string) *MetaAssignment {
	return &MetaAssignment{
		key: key,
	}
}

// Apply a value to a metadata key.
func (m *MetaAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	if ctx.Meta == nil {
		return errors.New("unable to assign metadata in the current context")
	}
	_, deleted := value.(query.Delete)
	if m.key == nil {
		if deleted {
			ctx.Meta.Iter(func(k, _ string) error {
				ctx.Meta.Delete(k)
				return nil
			})
		} else {
			if m, ok := value.(map[string]interface{}); ok {
				ctx.Meta.Iter(func(k, _ string) error {
					ctx.Meta.Delete(k)
					return nil
				})
				for k, v := range m {
					ctx.Meta.Set(k, query.IToString(v))
				}
			} else {
				return fmt.Errorf("setting root meta object requires object value, received: %T", value)
			}
		}
		return nil
	}
	if deleted {
		ctx.Meta.Delete(*m.key)
	} else {
		ctx.Meta.Set(*m.key, query.IToString(value))
	}
	return nil
}

// Target returns a representation of what the assignment targets.
func (m *MetaAssignment) Target() TargetPath {
	var path []string
	if m.key != nil {
		path = []string{*m.key}
	}
	return NewTargetPath(TargetMetadata, path...)
}

//------------------------------------------------------------------------------

// JSONAssignment creates a path within the structured message and assigns it a
// value.
type JSONAssignment struct {
	path []string
}

// NewJSONAssignment creates a new JSON assignment.
func NewJSONAssignment(path ...string) *JSONAssignment {
	return &JSONAssignment{
		path: path,
	}
}

// Apply a value to the target JSON path.
func (j *JSONAssignment) Apply(value interface{}, ctx AssignmentContext) error {
	_, deleted := value.(query.Delete)
	if !deleted {
		value = query.IClone(value)
	}
	if len(j.path) == 0 {
		*ctx.Value = value
	}
	if _, isNothing := (*ctx.Value).(query.Nothing); isNothing || *ctx.Value == nil {
		*ctx.Value = map[string]interface{}{}
	}
	gObj := gabs.Wrap(*ctx.Value)
	if deleted {
		gObj.Delete(j.path...)
	} else {
		gObj.Set(value, j.path...)
	}
	*ctx.Value = gObj.Data()
	return nil
}

// Target returns a representation of what the assignment targets.
func (j *JSONAssignment) Target() TargetPath {
	var path []string
	if len(j.path) > 0 {
		path = make([]string, len(j.path))
		copy(path, j.path)
	}
	return NewTargetPath(TargetValue, path...)
}

//------------------------------------------------------------------------------
