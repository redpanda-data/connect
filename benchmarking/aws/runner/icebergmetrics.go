// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseIcebergSeries reads a sink metric dump and returns a throughput series
// derived from the Iceberg table's committed-bytes counter.
//
// Dump format (one frame per poll interval, written by sinkTopology.MetricSidecar):
//
//	###timestamp=<unix-seconds>
//	total_files_size_bytes <cumulative-bytes>
//
// Throughput for frame i is (bytes[i]-bytes[i-1]) / (t[i]-t[i-1]) / MiB. Counter
// resets (current < previous, e.g. the table was dropped between sweep points)
// are skipped, mirroring ParseTopicSeries.
func ParseIcebergSeries(r io.Reader) ([]TopicPoint, error) {
	type frame struct {
		t       int64
		bytes   float64
		hasB    bool
		records float64
		hasR    bool
	}
	var frames []frame
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "###timestamp="):
			ts, err := strconv.ParseInt(strings.TrimPrefix(line, "###timestamp="), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp %q: %w", line, err)
			}
			frames = append(frames, frame{t: ts})
		case strings.HasPrefix(line, "total_files_size_bytes "):
			if len(frames) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "total_files_size_bytes ")), 64)
			if err != nil {
				return nil, fmt.Errorf("parse bytes %q: %w", line, err)
			}
			frames[len(frames)-1].bytes = v
			frames[len(frames)-1].hasB = true
		case strings.HasPrefix(line, "total_records "):
			if len(frames) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "total_records ")), 64)
			if err != nil {
				return nil, fmt.Errorf("parse records %q: %w", line, err)
			}
			frames[len(frames)-1].records = v
			frames[len(frames)-1].hasR = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(frames) == 0 {
		return nil, nil
	}
	baseT := frames[0].t
	var out []TopicPoint
	for i := 1; i < len(frames); i++ {
		prev, cur := frames[i-1], frames[i]
		if !prev.hasB || !cur.hasB {
			continue
		}
		interval := cur.t - prev.t
		delta := cur.bytes - prev.bytes
		if interval <= 0 || delta < 0 {
			continue // out-of-order or counter reset
		}
		var msgPerSec float64
		if prev.hasR && cur.hasR {
			if rd := cur.records - prev.records; rd >= 0 {
				msgPerSec = rd / float64(interval)
			}
		}
		out = append(out, TopicPoint{
			T:           int(cur.t - baseT),
			MBPerSec:    delta / float64(interval) / (1 << 20),
			MsgPerSec:   msgPerSec,
			IntervalSec: int(interval),
		})
	}
	return out, nil
}
