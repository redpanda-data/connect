package query

// TargetType represents a query target type, which is a source of information
// that a query might require, such as metadata, structured message contents,
// the context, etc.
type TargetType int

// TargetTypes.
const (
	TargetMetadata TargetType = iota
	TargetValue
	TargetRoot
	TargetVariable
)

// TargetPath represents a target type and segmented path that a query function
// references. An empty path indicates the root of the type is targeted.
type TargetPath struct {
	Type TargetType
	Path []string
}

// NewTargetPath constructs a new target path from a type and zero or more path
// segments.
func NewTargetPath(t TargetType, path ...string) TargetPath {
	return TargetPath{
		Type: t,
		Path: path,
	}
}

func aggregateTargetPaths(fns ...Function) func(ctx TargetsContext) (TargetsContext, []TargetPath) {
	return func(ctx TargetsContext) (TargetsContext, []TargetPath) {
		var paths []TargetPath
		for _, fn := range fns {
			if fn != nil {
				_, tmpPaths := fn.QueryTargets(ctx)
				paths = append(paths, tmpPaths...)
			}
		}
		return ctx, paths
	}
}

// TargetsContext describes the current Bloblang execution environment from the
// perspective of a particular query function in a way that allows it to
// determine which values it is targeting and the origins of those values.
//
// The environment consists of named maps that are globally accessible, the
// current value that is being executed upon by methods (when applicable), the
// general (main) context (referenced by the keyword `this`) and any other named
// contexts accessible at this point.
//
// Since it's possible for any query function to reference and return multiple
// target candidates (match expressions, etc) then each context and the current
// value are lists of paths, each being a candidate at runtime.
type TargetsContext struct {
	Maps map[string]Function

	currentValues []TargetPath
	mainContext   []TargetPath
	prevContext   *prevContextPath
	namedContext  *namedContextPath
}

type prevContextPath struct {
	paths []TargetPath
	next  *prevContextPath
}

type namedContextPath struct {
	name  string
	paths []TargetPath
	next  *namedContextPath
}

// Value returns the current value of the targets context, which is the path(s)
// being executed upon by methods.
func (ctx TargetsContext) Value() []TargetPath {
	return ctx.currentValues
}

// MainContext returns the path of the main context.
func (ctx TargetsContext) MainContext() []TargetPath {
	return ctx.mainContext
}

// NamedContext returns the path of a named context if it exists.
func (ctx TargetsContext) NamedContext(name string) []TargetPath {
	current := ctx.namedContext
	for current != nil {
		if current.name == name {
			return current.paths
		}
		current = current.next
	}
	return nil
}

// WithValues returns a targets context where the current value being executed
// upon by methods is set to something new.
func (ctx TargetsContext) WithValues(paths []TargetPath) TargetsContext {
	ctx.currentValues = paths
	return ctx
}

// WithValuesAsContext returns a targets context where the current value being
// executed upon by methods is now the main context. This happens when a query
// function is executed as a method, or within branches of match expressions.
func (ctx TargetsContext) WithValuesAsContext() TargetsContext {
	ctx.prevContext = &prevContextPath{
		paths: ctx.mainContext,
		next:  ctx.prevContext,
	}
	ctx.mainContext = ctx.currentValues
	ctx.currentValues = nil
	return ctx
}

// WithContextAsNamed moves the latest context into a named context and returns
// the context prior to that one to the main context. This is a way for named
// context mappings to correct the contexts so that the child query function
// returns the right paths.
func (ctx TargetsContext) WithContextAsNamed(name string) TargetsContext {
	previous := ctx.namedContext
	ctx.namedContext = &namedContextPath{
		name:  name,
		paths: ctx.mainContext,
		next:  previous,
	}
	if ctx.prevContext != nil {
		ctx.mainContext = ctx.prevContext.paths
		ctx.prevContext = ctx.prevContext.next
	}
	return ctx
}

// PopContext returns a targets context with the latest context dropped and the
// previous (when applicable) returned.
func (ctx TargetsContext) PopContext() TargetsContext {
	if ctx.prevContext != nil {
		ctx.mainContext = ctx.prevContext.paths
		ctx.prevContext = ctx.prevContext.next
	}
	return ctx
}
