// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"fmt"
	"os"
	"syscall"
)

func chroot(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if err := syscall.Chdir(path); err != nil {
		return fmt.Errorf("chdir: %w", err)
	}
	if err := syscall.Chroot(path); err != nil {
		return err
	}
	if err := syscall.Chdir("/"); err != nil {
		return fmt.Errorf("chdir /: %w", err)
	}

	return nil
}
