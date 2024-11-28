// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"fmt"
	"net"
	"strings"
)

func formatIP(ipStr string) string {
	if strings.Contains(ipStr, "/") {
		// If it has a mask, use ParseCIDR
		ip, network, err := net.ParseCIDR(ipStr)
		if err != nil {
			return "" // or handle error as needed
		}

		maskLen, _ := network.Mask.Size()
		return fmt.Sprintf("%s/%d", ip.String(), maskLen)
	} else {
		// Just an IP without mask
		ip := net.ParseIP(ipStr)
		if ip == nil {
			return "" // or handle error as needed
		}
		return ip.String()
	}
}
