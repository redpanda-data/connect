package subprocess

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper function to create a simple test command that prints output and exits
// This version is for Unix-like systems.
func createEchoCommand(message string, stream string, exitCode int) []string {
	// Use /bin/sh -c to execute the command string
	if stream == "stderr" {
		return []string{"/bin/sh", "-c", fmt.Sprintf("echo %q >&2; exit %d", message, exitCode)}
	}
	// Default to stdout
	return []string{"/bin/sh", "-c", fmt.Sprintf("echo %q; exit %d", message, exitCode)}
}

// Helper function to create a command that runs for a duration and then exits
// This version is for Unix-like systems.
func createSleepCommand(duration time.Duration) []string {
	return []string{"sleep", fmt.Sprintf("%f", duration.Seconds())}
}

func TestStartStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cmdArgs := createSleepCommand(2 * time.Second)
	sub, err := New(cmdArgs, nil)
	if err != nil {
		t.Fatalf("Failed to create subprocess: %v", err)
	}

	err = sub.Start()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.True(t, sub.IsRunning())
	err = sub.Close(ctx)
	require.NoError(t, err)
	require.False(t, sub.IsRunning())
	err = sub.Close(ctx)
	require.NoError(t, err)
}

func TestProcessExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cmdArgs := createSleepCommand(time.Second)
	sub, err := New(cmdArgs, nil)
	if err != nil {
		t.Fatalf("Failed to create subprocess: %v", err)
	}

	err = sub.Start()
	require.NoError(t, err)
	require.True(t, sub.IsRunning())
	time.Sleep(2 * time.Second)
	require.False(t, sub.IsRunning())
	err = sub.Close(ctx)
	require.NoError(t, err)
	require.False(t, sub.IsRunning())
}

func TestRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	cmdArgs := createSleepCommand(time.Second)
	sub, err := New(cmdArgs, nil)
	if err != nil {
		t.Fatalf("Failed to create subprocess: %v", err)
	}

	err = sub.Start()
	require.NoError(t, err)
	require.True(t, sub.IsRunning())
	time.Sleep(2 * time.Second)
	require.False(t, sub.IsRunning())
	require.NoError(t, sub.Close(ctx))
	err = sub.Start()
	require.NoError(t, err)
	require.True(t, sub.IsRunning())
	require.NoError(t, sub.Close(ctx))
}

func TestLoggingHooks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	logs := make(chan string)
	cmdArgs := createEchoCommand("whoot", "stdout", 0)
	sub, err := New(cmdArgs, nil, WithStdoutHook(func(line string) { logs <- line }))
	if err != nil {
		t.Fatalf("Failed to create subprocess: %v", err)
	}
	err = sub.Start()
	require.NoError(t, err)
	require.True(t, sub.IsRunning())
	line := <-logs
	require.Equal(t, "whoot", line)
	time.Sleep(time.Second)
	require.False(t, sub.IsRunning())
	require.NoError(t, sub.Close(ctx))
}
