package input

// SequenceShardedJoinConfig describes an optional mechanism for performing
// sharded joins of structured data resulting from the input sequence. This is a
// way to merge the structured fields of fragmented datasets within memory even
// when the overall size of the data surpasses the memory available on the
// machine.
//
// When configured the sequence of inputs will be consumed multiple times
// according to the number of iterations, and each iteration will process an
// entirely different set of messages by sharding them by the ID field.
//
// Each message must be structured (JSON or otherwise processed into a
// structured form) and the fields will be aggregated with those of other
// messages sharing the ID. At the end of each iteration the joined messages are
// flushed downstream before the next iteration begins.
type SequenceShardedJoinConfig struct {
	Type          string `json:"type" yaml:"type"`
	IDPath        string `json:"id_path" yaml:"id_path"`
	Iterations    int    `json:"iterations" yaml:"iterations"`
	MergeStrategy string `json:"merge_strategy" yaml:"merge_strategy"`
}

// NewSequenceShardedJoinConfig creates a new sequence sharding configuration
// with default values.
func NewSequenceShardedJoinConfig() SequenceShardedJoinConfig {
	return SequenceShardedJoinConfig{
		Type:          "none",
		IDPath:        "",
		Iterations:    1,
		MergeStrategy: "array",
	}
}

// SequenceConfig contains configuration values for the Sequence input type.
type SequenceConfig struct {
	ShardedJoin SequenceShardedJoinConfig `json:"sharded_join" yaml:"sharded_join"`
	Inputs      []Config                  `json:"inputs" yaml:"inputs"`
}

// NewSequenceConfig creates a new SequenceConfig with default values.
func NewSequenceConfig() SequenceConfig {
	return SequenceConfig{
		ShardedJoin: NewSequenceShardedJoinConfig(),
		Inputs:      []Config{},
	}
}
