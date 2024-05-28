package ockam

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// Run `ockam ...` commands
func runCommand(capture bool, arg ...string) (string, string, error) {
	bin, err := findCommandBinary()
	if err != nil {
		return "", "", fmt.Errorf("failed to find Ockam Command binary: %v", err)
	}

	cmd := exec.Command(bin, arg...)
	cmd.Env = append(os.Environ(),
		"NO_INPUT=true",
		"NO_COLOR=true",
		"OCKAM_DISABLE_UPGRADE_CHECK=true",
		"OCKAM_OPENTELEMETRY_EXPORT=false",
	)

	var stdoutBuf, stderrBuf bytes.Buffer
	if capture {
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
	} else {
		devNull, err := os.Open(os.DevNull)
		if err != nil {
			return "", "", fmt.Errorf("failed to open %s: %v", os.DevNull, err)
		}
		defer devNull.Close()

		cmd.Stdout = devNull
		cmd.Stderr = devNull
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	err = cmd.Run()
	stdout := stdoutBuf.String()
	stderr := stderrBuf.String()
	if err != nil {
		errMsg := fmt.Sprintf("failed to run the command: %s, error: %v\nstdout:\n%s\nstderr:\n%s",
			cmd.String(), err, stdout, stderr)
		return stdout, stderr, errors.New(errMsg)
	}

	return stdout, stderr, nil
}

// Returns the path to the Ockam Command binary.
// If it's not found, it will be downloaded and installed.
func setupCommand() (string, error) {
	bin, err := findCommandBinary()
	if err == nil {
		return bin, nil
	}

	err = installCommand()
	if err != nil {
		return "", fmt.Errorf("failed to install Ockam Command: %v", err)
	}

	return findCommandBinary()
}

// Returns the path to the Ockam Command binary or an error if it can't find the binary.
func findCommandBinary() (string, error) {
	// If the OCKAM environment variable is set, assume that as the path of the Ockam Command binary.
	command := os.Getenv("OCKAM")
	if command != "" {
		return command, nil
	}

	// If ockam is in path, assume that as the Ockam Command binary.
	_, err := exec.LookPath("ockam")
	if err == nil {
		return "ockam", nil
	}

	// Try to find the path of Ockam Command by running `command -v ockam`
	shell, err := shell()
	if err == nil {
		cmdToFindBinary := "command -v ockam"

		// If ockamHome()/env file is present and readable, source it before running `command -v ockam`
		// This may be helpful when Ockam Command was installed using the Ockam Command install script.
		envFile, err := envFile()
		if err == nil {
			cmdToFindBinary = "source " + envFile + " && " + cmdToFindBinary
		}

		cmd := exec.Command(shell, "-c", cmdToFindBinary)
		output, err := cmd.Output()
		if err == nil {
			path := strings.TrimSpace(string(output))
			if path != "" {
				return path, nil
			}
		}
	}

	// If ockamHome() + "/bin/ockam" exists, return its path
	path := ockamHome() + "/bin/ockam"
	_, err = os.Stat(path)
	if err == nil {
		return path, nil
	}

	return "", errors.New("failed to find Ockam Command binary")
}

// Installs Ockam Command.
//
// If bash is not available, directly download and install the binary.
// If bash is available, install using the install script.
func installCommand() error {
	_, err := exec.LookPath("bash")
	if err != nil {
		return downloadAndInstall()
	} else {
		return downloadAndInstallWithInstallScript()
	}
}

func downloadAndInstall() error {
	binaryType, err := pickBinaryType()
	if err != nil {
		return err
	}
	url := "https://github.com/build-trust/ockam/releases/latest/download/ockam." + binaryType

	version := os.Getenv("OCKAM_VERSION")
	if version != "" {
		url = "https://github.com/build-trust/ockam/releases/download/ockam_" + version + "/ockam." + binaryType
	}

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download the binary %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got HTTP response with status code != 200, while downloading %s: %v", url, resp.StatusCode)
	}

	binary := ockamHome() + "/bin/ockam"
	out, err := os.Create(binary)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", binary, err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy downloaded contents to file %s: %v", binary, err)
	}

	err = os.Chmod(binary, 0700)
	if err != nil {
		return fmt.Errorf("failed to change permissions of the file %s: %v", binary, err)
	}
	return nil
}

func pickBinaryType() (string, error) {
	binaries := map[string]string{
		"darwin/arm64": "aarch64-apple-darwin",
		"darwin/amd64": "x86_64-apple-darwin",
		"linux/arm64":  "aarch64-unknown-linux-musl",
		"linux/armv7":  "armv7-unknown-linux-musleabihf",
		"linux/amd64":  "x86_64-unknown-linux-gnu",
	}

	os := runtime.GOOS
	arch := runtime.GOARCH
	binary, exists := binaries[fmt.Sprintf("%s/%s", os, arch)]
	if !exists {
		return "", fmt.Errorf("no available binary for: %s/%s", os, arch)
	}

	return binary, nil
}

func downloadAndInstallWithInstallScript() error {
	// Download the install script.
	resp, err := http.Get("https://install.command.ockam.io")
	if err != nil {
		return fmt.Errorf("failed to download the install script: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got HTTP response with status code != 200, while downloading the install script: %v", resp.StatusCode)
	}

	// Save the install script to a temporary file.
	tmpFile, err := os.CreateTemp("", "install-ockam-*.sh")
	if err != nil {
		return fmt.Errorf("failed to create temporary file for the install script: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy install script to a temporary file: %v", err)
	}
	err = os.Chmod(tmpFile.Name(), 0700)
	if err != nil {
		return fmt.Errorf("failed to change permissions on the install script to 0700: %v", err)
	}

	// Prepare the install script invocation command
	c := []string{tmpFile.Name()}
	version := os.Getenv("OCKAM_VERSION")
	if version != "" {
		c = append(c, "--version", version)
	}

	// Run the install script
	cmd := exec.Command("bash", c...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to execute the install script: %v", err)
	}

	return nil
}

// Returns the name of a shell executable, "bash" or "sh".
//
// It returns the name of a shell only if an executable with that name is found in $PATH.
// Returns an error of neither bash or sh are found in $PATH. Which may happen in environments like a docker container.
func shell() (string, error) {
	shells := []string{"bash", "sh"}
	for _, s := range shells {
		_, err := exec.LookPath(s)
		if err == nil {
			return s, nil
		}
	}
	return "", errors.New("failed to find bash or sh in path")
}

// Returns the path to the environment file that is used to add Ockam Command to $PATH, in a shell.
//
// This file may be found at ockamHome()/env. This function first tries to open the file at that path in read-only mode.
// If opening the env file succeeds, this function returns its path, otherwise it returns an error.
func envFile() (string, error) {
	envFile := ockamHome() + "/env"

	// Check if the env file can be opened for reading
	file, err := os.Open(envFile)
	if err != nil {
		return "", fmt.Errorf("failed to open env file %s: %v", envFile, err)
	}
	defer file.Close()

	return envFile, nil
}

// Returns the path to Ockam Command's home directory.
func ockamHome() string {
	home := os.Getenv("OCKAM_HOME")
	if home != "" {
		return home
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ".ockam"
	}

	return filepath.Join(homeDir, ".ockam")
}

func findAvailableLocalTCPAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	_ = listener.Close()

	return address, nil
}

func localTCPAddressIsTaken(address string) bool {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return true
	}
	_ = listener.Close()
	return false
}
