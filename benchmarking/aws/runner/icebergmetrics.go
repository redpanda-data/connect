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
// Throughput for frame i is (bytes[i]-bytes[i-1]) / (t[i]-t[i-1]) / bytesPerMB. Counter
// resets (current < previous, e.g. the table was dropped between sweep points)
// are skipped, mirroring ParseTopicSeries.
func ParseIcebergSeries(r io.Reader) ([]TopicPoint, error) {
	type frame struct {
		t       int64
		bytes   float64
		hasB    bool
		records float64
		hasR    bool
		// Written-work counters (sum of snapshot added-records /
		// added-files-size, emitted by newer sidecars). Preferred over the
		// net totals when both frames carry them: net totals freeze for
		// keyed workloads once every key exists in the table, so a
		// copy-on-write sink that is rewriting flat out reads as zero.
		// Dumps without these lines parse exactly as before.
		wBytes   float64
		hasWB    bool
		wRecords float64
		hasWR    bool
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
		case strings.HasPrefix(line, "written_files_size_bytes "):
			if len(frames) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "written_files_size_bytes ")), 64)
			if err != nil {
				return nil, fmt.Errorf("parse written bytes %q: %w", line, err)
			}
			frames[len(frames)-1].wBytes = v
			frames[len(frames)-1].hasWB = true
		case strings.HasPrefix(line, "written_records "):
			if len(frames) == 0 {
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, "written_records ")), 64)
			if err != nil {
				return nil, fmt.Errorf("parse written records %q: %w", line, err)
			}
			frames[len(frames)-1].wRecords = v
			frames[len(frames)-1].hasWR = true
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
		// Written-work counters take precedence per metric when both frames
		// carry them (see the frame struct comment); the net totals remain
		// the fallback so pre-written dumps parse unchanged.
		bytesDelta := cur.bytes - prev.bytes
		if prev.hasWB && cur.hasWB {
			bytesDelta = cur.wBytes - prev.wBytes
		}
		if interval <= 0 || bytesDelta < 0 {
			continue // out-of-order or counter reset
		}
		var msgPerSec float64
		if prev.hasWR && cur.hasWR {
			if rd := cur.wRecords - prev.wRecords; rd >= 0 {
				msgPerSec = rd / float64(interval)
			}
		} else if prev.hasR && cur.hasR {
			if rd := cur.records - prev.records; rd >= 0 {
				msgPerSec = rd / float64(interval)
			}
		}
		out = append(out, TopicPoint{
			T:           int(cur.t - baseT),
			MBPerSec:    bytesDelta / float64(interval) / bytesPerMB,
			MsgPerSec:   msgPerSec,
			IntervalSec: int(interval),
		})
	}
	return out, nil
}
