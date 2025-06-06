// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"syscall"
)

// chroot changes the root directory to the provided path and then changes
// the current directory to "/".
//
// NOTE: This function will only work if the binary is running with
// sufficient privileges to call syscall.Chroot. If the binary does not
// have the necessary privileges, this function will return an error.
func chroot(path string) error {
	if err := syscall.Chroot(path); err != nil {
		return err
	}
	if err := syscall.Chdir("/"); err != nil {
		return err
	}

	return nil
}
