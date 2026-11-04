// Copyright 2026 Redpanda Data, Inc.
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

//go:build unix

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// interruptGrace is how long the process group of an interrupted go test can
// run after SIGINT. After that, the group gets SIGKILL.
const interruptGrace = 30 * time.Second

// interruptProcessGroup starts cmd in its own process group, and makes the
// cancel of its context send SIGINT to that group. The caller must call the
// returned func after cmd.Wait.
//
// The go command ignores SIGINT and waits for the test binary, which must get
// the signal itself. The default cancel kills the go command only, so the test
// binary keeps running and uses its containers while dockerCleanup removes
// them.
func interruptProcessGroup(cmd *exec.Cmd) (waited func()) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	done := make(chan struct{})
	cmd.Cancel = func() error {
		pgid := -cmd.Process.Pid
		go func() {
			select {
			case <-done:
			case <-time.After(interruptGrace):
				_ = syscall.Kill(pgid, syscall.SIGKILL)
			}
		}()
		err := syscall.Kill(pgid, syscall.SIGINT)
		if errors.Is(err, syscall.ESRCH) {
			// The group is gone. Tell exec that this is not an error.
			return os.ErrProcessDone
		}
		return err
	}
	return func() { close(done) }
}
