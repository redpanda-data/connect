// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import "fmt"

// minBytesPerRecordFraction is the fraction of dataset.row_size_bytes below
// which a sink's stored bytes per consumed record is considered implausible.
// Compression legitimately shrinks rows, but the seeders emit high-entropy
// payloads that compress to well over half their size, so a few percent
// means records were consumed and not stored.
const minBytesPerRecordFraction = 0.05

// lowBytesPerRecordWarning flags a sink point whose consumer group advanced
// while almost nothing reached the sink. It exists for connectors configured
// to tolerate errors (Confluent S3 with errors.tolerance=all): a failing
// converter then skips every record silently while the committed offsets, the
// records half of the sidecar's frames, keep moving, so msg/s alone reads as
// healthy throughput with zero stored bytes. Returns "" when the point looks
// sane or there is too little data to judge. Advisory only: the caller prints
// it and never fails the run.
func lowBytesPerRecordWarning(engine string, series []TopicPoint, rowSizeBytes int) string {
	if rowSizeBytes <= 0 {
		return ""
	}
	var bytes, records float64
	for _, p := range series {
		secs := float64(p.IntervalSec)
		bytes += p.MBPerSec * bytesPerMB * secs
		records += p.MsgPerSec * secs
	}
	if records <= 0 {
		return ""
	}
	perRecord := bytes / records
	floor := float64(rowSizeBytes) * minBytesPerRecordFraction
	if perRecord >= floor {
		return ""
	}
	return fmt.Sprintf("###WARN %s: consumer offsets advanced %.0f records but the sink stored only %.0f bytes (%.1f B/record, expected at least %.0f for %d B rows): records are being consumed and dropped (check errors.tolerance and the worker log); the msg/s figure for this point is NOT trustworthy, use the stored bytes",
		engine, records, bytes, perRecord, floor, rowSizeBytes)
}
