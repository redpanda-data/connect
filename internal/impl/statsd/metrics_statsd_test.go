// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statsd

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatsdGlobalTagsDatadog(t *testing.T) {
	// Create a UDP listener to capture statsd metrics
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)

	conn, err := net.ListenUDP("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	port := conn.LocalAddr().(*net.UDPAddr).Port

	pConf, err := statsdSpec().ParseYAML(fmt.Sprintf(`
address: 127.0.0.1:%d
flush_period: 10ms
tag_format: datadog
tags:
  hostname: localhost
  zone: danger
`, port), nil)
	require.NoError(t, err)

	s, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)

	// Send a counter metric
	counter := s.NewCounterCtor("test_counter")()
	counter.Incr(1)

	// Send a gauge metric
	gauge := s.NewGaugeCtor("test_gauge")()
	gauge.Set(42)

	// Send a timer metric
	timer := s.NewTimerCtor("test_timer")()
	timer.Timing(100)

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Read the metrics from the UDP listener
	buf := make([]byte, 4096)
	err = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	require.NoError(t, err)

	n, err := conn.Read(buf)
	require.NoError(t, err)

	received := string(buf[:n])

	// Close the metrics client
	require.NoError(t, s.Close(context.Background()))

	// Datadog format: metric_name:value|type|#tag1:value1,tag2:value2
	// Verify global tags are present in the metrics
	assert.Contains(t, received, "hostname:localhost", "should contain hostname global tag")
	assert.Contains(t, received, "zone:danger", "should contain zone global tag")
}

func TestStatsdGlobalTagsInfluxDB(t *testing.T) {
	// Create a UDP listener to capture statsd metrics
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)

	conn, err := net.ListenUDP("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	port := conn.LocalAddr().(*net.UDPAddr).Port

	pConf, err := statsdSpec().ParseYAML(fmt.Sprintf(`
address: 127.0.0.1:%d
flush_period: 10ms
tag_format: influxdb
tags:
  hostname: localhost
  zone: danger
`, port), nil)
	require.NoError(t, err)

	s, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)

	// Send a counter metric
	counter := s.NewCounterCtor("test_counter")()
	counter.Incr(1)

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Read the metrics from the UDP listener
	buf := make([]byte, 4096)
	err = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	require.NoError(t, err)

	n, err := conn.Read(buf)
	require.NoError(t, err)

	received := string(buf[:n])

	// Close the metrics client
	require.NoError(t, s.Close(context.Background()))

	// InfluxDB format: metric_name,tag1=value1,tag2=value2:value|type
	// Verify global tags are present in the metrics
	assert.Contains(t, received, "hostname=localhost", "should contain hostname global tag in InfluxDB format")
	assert.Contains(t, received, "zone=danger", "should contain zone global tag in InfluxDB format")
}

func TestStatsdGlobalTagsWithLabelTags(t *testing.T) {
	// Create a UDP listener to capture statsd metrics
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)

	conn, err := net.ListenUDP("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	port := conn.LocalAddr().(*net.UDPAddr).Port

	pConf, err := statsdSpec().ParseYAML(fmt.Sprintf(`
address: 127.0.0.1:%d
flush_period: 10ms
tag_format: datadog
tags:
  hostname: localhost
`, port), nil)
	require.NoError(t, err)

	s, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)

	// Send a counter metric with label tags
	counter := s.NewCounterCtor("test_counter", "method", "status")("GET", "200")
	counter.Incr(1)

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Read the metrics from the UDP listener
	buf := make([]byte, 4096)
	err = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	require.NoError(t, err)

	n, err := conn.Read(buf)
	require.NoError(t, err)

	received := string(buf[:n])

	// Close the metrics client
	require.NoError(t, s.Close(context.Background()))

	// Verify both global tags and label tags are present
	assert.Contains(t, received, "hostname:localhost", "should contain hostname global tag")
	assert.Contains(t, received, "method:GET", "should contain method label tag")
	assert.Contains(t, received, "status:200", "should contain status label tag")
}

func TestStatsdNoGlobalTags(t *testing.T) {
	// Create a UDP listener to capture statsd metrics
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)

	conn, err := net.ListenUDP("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	port := conn.LocalAddr().(*net.UDPAddr).Port

	pConf, err := statsdSpec().ParseYAML(fmt.Sprintf(`
address: 127.0.0.1:%d
flush_period: 10ms
tag_format: datadog
`, port), nil)
	require.NoError(t, err)

	s, err := newStatsdFromParsed(pConf, nil)
	require.NoError(t, err)

	// Send a counter metric with label tags
	counter := s.NewCounterCtor("test_counter", "method")("GET")
	counter.Incr(1)

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Read the metrics from the UDP listener
	buf := make([]byte, 4096)
	err = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	require.NoError(t, err)

	n, err := conn.Read(buf)
	require.NoError(t, err)

	received := string(buf[:n])

	// Close the metrics client
	require.NoError(t, s.Close(context.Background()))

	// Verify only label tags are present (no extra global tags)
	assert.Contains(t, received, "method:GET", "should contain method label tag")
	// Count occurrences of tags - should only have the one label tag
	tagCount := strings.Count(received, ":")
	// We expect test_counter:1|c|#method:GET, so 2 colons: one for value, one for tag
	assert.Equal(t, 2, tagCount, "should have exactly 2 colons (value and one tag)")
}

func TestStatsdTagsHelperFunction(t *testing.T) {
	// Test the tags helper function
	t.Run("matching labels and values", func(t *testing.T) {
		result := tags([]string{"a", "b", "c"}, []string{"1", "2", "3"})
		assert.Len(t, result, 3)
	})

	t.Run("mismatched labels and values", func(t *testing.T) {
		result := tags([]string{"a", "b"}, []string{"1"})
		assert.Nil(t, result)
	})

	t.Run("empty labels and values", func(t *testing.T) {
		result := tags([]string{}, []string{})
		assert.Empty(t, result, 0)
	})
}
