// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subprocess

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ErrProcessAlreadyStarted is returned when trying to start a subprocess that is already running.
var ErrProcessAlreadyStarted = errors.New("subprocess already started")

// Option is a function that can configure a SubProcess.
type Option func(*Subprocess)

// WithCwd allows you to configure the working directory for the subprocess.
func WithCwd(dir string) Option {
	return func(s *Subprocess) {
		s.cwd = dir
	}
}

// WithLogger allows providing a custom logger for internal library messages.
func WithLogger(logger *service.Logger) Option {
	return func(s *Subprocess) {
		s.logger = logger
	}
}

// WithStdoutHook allows providing a custom logger for stdout messages.
func WithStdoutHook(hook func(line string)) Option {
	return func(s *Subprocess) {
		s.stderrHook = hook
	}
}

// WithStderrHook allows providing a custom logger for stderr messages.
func WithStderrHook(hook func(line string)) Option {
	return func(s *Subprocess) {
		s.stdoutHook = hook
	}
}

type Subprocess struct {
	cmdArgs    []string
	env        map[string]string
	stdoutHook func(line string)
	stderrHook func(line string)
	logger     *service.Logger
	cwd        string

	cmd    *exec.Cmd
	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new SubProcess instance.
func New(
	cmd []string,
	env map[string]string,
	options ...Option,
) (*Subprocess, error) {
	if len(cmd) == 0 {
		return nil, errors.New("command cannot be empty")
	}
	s := &Subprocess{
		cmdArgs: cmd,
		env:     env,
		logger:  nil,
	}
	for _, option := range options {
		option(s)
	}
	return s, nil
}

func (s *Subprocess) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		return ErrProcessAlreadyStarted
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, s.cmdArgs[0], s.cmdArgs[1:]...)
	cmd.Dir = s.cwd
	cmd.Env = []string{}
	for k, v := range s.env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		stdoutPipe.Close()
		cancel()
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		stdoutPipe.Close()
		stderrPipe.Close()
		cancel()
		return fmt.Errorf("failed to start command: %w", err)
	}
	s.logger.Debugf("Subprocess started with PID: %d", cmd.Process.Pid)
	s.wg.Add(3) // For stdout, stderr, and process wait goroutines
	go s.readOutput(stdoutPipe, false)
	go s.readOutput(stderrPipe, true)
	go func() {
		defer s.wg.Done()
		err := cmd.Wait()
		s.logger.Debugf("Subprocess with PID %d exited with error: %v", cmd.Process.Pid, err)
	}()
	s.cmd = cmd
	s.cancel = cancel
	return nil
}

func (s *Subprocess) readOutput(pipe io.Reader, isStderr bool) {
	defer s.wg.Done()
	src := map[bool]string{false: "stdout", true: "stderr"}[isStderr]
	log := s.logger.With("source", src)
	scanner := bufio.NewScanner(pipe)
	hook := func(line string) {}
	if !isStderr && s.stdoutHook != nil {
		hook = s.stdoutHook
	} else if isStderr && s.stderrHook != nil {
		hook = s.stderrHook
	}
	for scanner.Scan() {
		line := scanner.Text()
		hook(line)
		log.Infof("%s", line)
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Warnf("error reading from subprocess: %v", err)
	}
}

func (s *Subprocess) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil {
		return false
	}
	if s.cmd.ProcessState != nil && s.cmd.ProcessState.Exited() {
		return false
	}
	return true
}

func (s *Subprocess) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd == nil || s.cancel == nil {
		s.logger.Tracef("Close called on a subprocess that is not running or already closed.")
		return nil // Not running or already closed
	}

	s.logger.Debugf("Attempting to gracefully shut down subprocess with PID %d...", s.cmd.Process.Pid)
	if s.cmd.Process != nil {
		if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
			s.logger.Warnf("Failed to send interrupt signal to subprocess PID %d: %v. Attempting to kill.", s.cmd.Process.Pid, err)
			if err := s.cmd.Process.Kill(); err != nil {
				s.logger.Errorf("Failed to kill subprocess PID %d: %v", s.cmd.Process.Pid, err)
			}
		}
	}
	// Use the provided context for waiting for the process to exit
	done := make(chan struct{})
	go func() {
		s.wg.Wait() // Wait for all goroutines (output readers and waitProcess) to finish
		close(done)
	}()

	select {
	case <-done:
		s.logger.Tracef("Subprocess goroutines finished.")
	case <-ctx.Done():
		s.logger.Tracef("Context cancelled while waiting for subprocess PID %d to exit.", s.cmd.Process.Pid)
		// The subprocess might still be running if it didn't respond to signals and the context timed out.
		if s.cmd.Process != nil && s.cmd.ProcessState == nil || (s.cmd.ProcessState != nil && !s.cmd.ProcessState.Exited()) {
			s.logger.Warnf("Subprocess PID %d did not exit within context deadline, attempting forceful kill.", s.cmd.Process.Pid)
			if err := s.cmd.Process.Kill(); err != nil {
				s.logger.Errorf("Failed to forcefully kill subprocess PID %d: %v", s.cmd.Process.Pid, err)
			}
		}
		return ctx.Err()
	}
	s.cancel()
	s.cmd = nil
	s.logger.Tracef("Subprocess closed successfully.")
	return nil
}
