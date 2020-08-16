package query

// TargetType represents a query target type, which is a source of information
// that a query might require, such as metadata, structured message contents,
// the context, etc.
type TargetType int

// TargetTypes
const (
	TargetMetadata TargetType = iota
	TargetValue
	TargetVariable
)

// TargetPath represents a target type and segmented path that a query function
// references. An empty path indicates the root of the type is targetted.
type TargetPath struct {
	Type TargetType
	Path []string
}

// NewTargetPath constructs a new target path from a type and zero or more path
// segments.
func NewTargetPath(t TargetType, Path ...string) TargetPath {
	return TargetPath{
		Type: t,
		Path: Path,
	}
}

func aggregateTargetPaths(fns ...Function) func() []TargetPath {
	return func() []TargetPath {
		var paths []TargetPath
		for _, fn := range fns {
			paths = append(paths, fn.QueryTargets()...)
		}
		return paths
	}
}

// Expand a slice of target paths with another, as if the reference targets make
// up the context of those paths.
func expandTargetPaths(paths, with []TargetPath) []TargetPath {
	var refValuePaths [][]string
	for _, refTarget := range with {
		if refTarget.Type == TargetValue {
			refValuePaths = append(refValuePaths, refTarget.Path)
		}
	}
	if len(refValuePaths) == 0 {
		return paths
	}

	var expandedPaths []TargetPath
	for _, target := range paths {
		if target.Type == TargetValue {
			for _, refValuePath := range refValuePaths {
				var newPath []string
				newPath = append(newPath, refValuePath...)
				newPath = append(newPath, target.Path...)
				expandedPaths = append(expandedPaths, NewTargetPath(TargetValue, newPath...))
			}
		} else {
			expandedPaths = append(expandedPaths, target)
		}
	}
	return expandedPaths
}
