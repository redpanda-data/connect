package metrics

// Flat is an interface for setting metrics via flat paths.
type Flat interface {
	// Flat is an interface for setting metrics via flat paths.
	// Incr increments a metric by an amount.
	Incr(path string, count int64) error

	// Decr decrements a metric by an amount.
	Decr(path string, count int64) error

	// Timing sets a timing metric.
	Timing(path string, delta int64) error

	// Gauge sets a gauge metric.
	Gauge(path string, value int64) error

	// Close stops aggregating stats and cleans up resources.
	Close() error
}
