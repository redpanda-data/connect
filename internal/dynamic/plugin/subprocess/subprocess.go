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

// SubProcess defines the interface for managing a subprocess.
type SubProcess interface {
	// Start launches the subprocess and begins monitoring. It returns an error
	// if the process fails to start initially.
	Start() error
	// IsRunning checks if the subprocess is currently running.
	// Close MUST be called even if IsRunning returns false.
	IsRunning() bool
	// Close attempts to gracefully shut down the subprocess. It waits for the process
	// to exit and cleans up resources.
	Close(ctx context.Context) error
}

// Option is a function that can configure a SubProcess.
type Option func(*subprocess)

// WithLogger allows providing a custom logger for internal library messages.
func WithLogger(logger *service.Logger) Option {
	return func(s *subprocess) {
		s.logger = logger
	}
}

// WithStdoutHook allows providing a custom logger for stdout messages.
func WithStdoutHook(hook func(line string)) Option {
	return func(s *subprocess) {
		s.stdoutHook = hook
	}
}

// WithStderrHook allows providing a custom logger for stderr messages.
func WithStderrHook(hook func(line string)) Option {
	return func(s *subprocess) {
		s.stdoutHook = hook
	}
}

type subprocess struct {
	cmdArgs    []string
	env        map[string]string
	stdoutHook func(line string)
	stderrHook func(line string)
	logger     *service.Logger

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
) (SubProcess, error) {
	if len(cmd) == 0 {
		return nil, errors.New("command cannot be empty")
	}
	s := &subprocess{
		cmdArgs: cmd,
		env:     env,
		logger:  nil,
	}
	for _, option := range options {
		option(s)
	}
	return s, nil
}

func (s *subprocess) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		return ErrProcessAlreadyStarted
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, s.cmdArgs[0], s.cmdArgs[1:]...)
	cmd.Env = []string{}
	for k, v := range s.env {
		cmd.Env = append(s.cmd.Env, fmt.Sprintf("%s=%s", k, v))
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

func (s *subprocess) readOutput(pipe io.Reader, isStderr bool) {
	defer s.wg.Done()
	src := map[bool]string{false: "stdout", true: "stderr"}[isStderr]
	log := s.logger.With("source", src)
	scanner := bufio.NewScanner(pipe)
	hook := func(line string) {}
	if !isStderr && s.stdoutHook != nil {
		hook = s.stdoutHook
	}
	if isStderr && s.stderrHook != nil {
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

func (s *subprocess) IsRunning() bool {
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

func (s *subprocess) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cmd == nil || s.cancel == nil {
		s.logger.Tracef("Close called on a subprocess that is not running or already closed.")
		return nil // Not running or already closed
	}

	s.logger.Debugf("Attempting to gracefully shut down subprocess with PID %d...", s.cmd.Process.Pid)
	if s.cmd.Process != nil {
		if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
			s.logger.Tracef("Failed to send interrupt signal to subprocess PID %d: %v. Attempting to kill.", s.cmd.Process.Pid, err)
			if err := s.cmd.Process.Kill(); err != nil {
				s.logger.Warnf("Failed to kill subprocess PID %d: %v", s.cmd.Process.Pid, err)
			}
		}
	}
	// Use the provided context for waiting for the process to exit
	waitChan := make(chan struct{})
	go func() {
		s.wg.Wait() // Wait for all goroutines (output readers and waitProcess) to finish
		close(waitChan)
	}()

	select {
	case <-waitChan:
		s.logger.Tracef("Subprocess goroutines finished.")
	case <-ctx.Done():
		s.logger.Tracef("Context cancelled while waiting for subprocess PID %d to exit.", s.cmd.Process.Pid)
		// The subprocess might still be running if it didn't respond to signals and the context timed out.
		if s.cmd.Process != nil && s.cmd.ProcessState == nil || (s.cmd.ProcessState != nil && !s.cmd.ProcessState.Exited()) {
			s.logger.Tracef("Subprocess PID %d did not exit within context deadline, attempting forceful kill.", s.cmd.Process.Pid)
			if err := s.cmd.Process.Kill(); err != nil {
				s.logger.Warnf("Failed to forcefully kill subprocess PID %d: %v", s.cmd.Process.Pid, err)
			}
		}
		return ctx.Err()
	}
	s.cancel()
	s.cmd = nil
	s.logger.Tracef("Subprocess closed successfully.")
	return nil
}
