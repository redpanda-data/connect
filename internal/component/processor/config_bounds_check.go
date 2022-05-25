package processor

// BoundsCheckConfig contains configuration fields for the BoundsCheck
// processor.
type BoundsCheckConfig struct {
	MaxParts    int `json:"max_parts" yaml:"max_parts"`
	MinParts    int `json:"min_parts" yaml:"min_parts"`
	MaxPartSize int `json:"max_part_size" yaml:"max_part_size"`
	MinPartSize int `json:"min_part_size" yaml:"min_part_size"`
}

// NewBoundsCheckConfig returns a BoundsCheckConfig with default values.
func NewBoundsCheckConfig() BoundsCheckConfig {
	return BoundsCheckConfig{
		MaxParts:    100,
		MinParts:    1,
		MaxPartSize: 1 * 1024 * 1024 * 1024, // 1GB
		MinPartSize: 1,
	}
}
