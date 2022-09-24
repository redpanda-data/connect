package mapping

// TargetType represents a mapping target type, which is a destination for a
// query result to be mapped into a message.
type TargetType int

// TargetTypes.
const (
	TargetMetadata TargetType = iota
	TargetValue
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
