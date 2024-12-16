package spannercdc

import "time"

// Config spanner CDC struct.
type Config struct {
	DSN               string
	Stream            string
	MetadataTable     *string
	Start             *time.Time
	End               *time.Time
	HeartbeatInterval *time.Duration
	PartitionDSN      *string
	Priority          int32
}
