package input

import (
	"errors"
	"fmt"
	"github.com/pkg/sftp"
	"log"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	benthosLog "github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ssh"
)

type scenario struct {
	Name string
	Conf *SFTPConfig
}

var sshClient *sftp.Client
var sshUsername = "foo"
var sshPassword = "pass"
var sshDirectory = "/upload"
var sshPort int

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			"foo:pass:1001:100:upload",
		},
	})

	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		sshPort, err = strconv.Atoi(resource.GetPort("22/tcp"))
		if err != nil {
			return err
		}

		isConnected := ConnectToSSHServer("localhost", sshPort)
		if !isConnected {
			return errors.New("failed to connect to SSH server")
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestProcessFile(t *testing.T) {
	filePath := path.Join(sshDirectory, "test.txt")
	testCase := scenario{
		Name: "good conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: "foo",
				Secret:   "pass",
			},
			WatcherMode:            false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	GenerateTestFile(filePath, "This is a test file")
	time.Sleep(time.Second * 1)

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	ValidateMessage(t, proc, "This is a test file", filePath, 1)
}

func TestNoServer(t *testing.T) {
	filepath := path.Join(sshDirectory, "test.txt")
	testCase := scenario{
		Name: "no server",
		Conf: &SFTPConfig{
			Server:   "invalid_server",
			Port:     sshPort,
			Filepath: filepath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.False(t, proc.Connected())
}

func TestInvalidCredentials(t *testing.T) {
	testCase := scenario{
		Name: "invalid credentials",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: path.Join(sshDirectory, "test.txt"),
			Credentials: SFTPCredentials{
				Username: "invaliduser",
				Secret:   "invalidsecret",
			},
			WatcherMode:            false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.False(t, proc.Connected())
}

func TestFileNotFound(t *testing.T) {
	testCase := scenario{
		Name: "file not found",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: path.Join(sshDirectory, "missingfile.txt"),
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.False(t, proc.Connected())
}

func TestWatcherMode(t *testing.T) {
	filePath := path.Join(sshDirectory, "watcher_test.txt")
	testCase := scenario{
		Name: "watcher mode conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            true,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	defer DeleteTestFile(filePath)

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	GenerateTestFile(filePath, "This is a test\n")
	time.Sleep(time.Second * 1)

	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	ValidateMessage(t, proc, "This is a test\n", filePath, 1)

	// Alter the file and validate that the new line was received
	UpdateTestFile(filePath, "This is a test\nTest second line")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Test second line", filePath, 2)
}

func TestDelimiterIncludeHeader(t *testing.T) {
	filePath := path.Join(sshDirectory, "test.csv")
	testCase := scenario{
		Name: "delimiter include header conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            false,
			MessageDelimiter:       "\n",
			IncludeHeader:          true,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	GenerateTestFile(filePath, "First Name,Last Name\nJohn,Smith\nJane,Doe")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	ValidateMessage(t, proc, "First Name,Last Name", filePath, 1)
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "John,Smith", filePath, 2)
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Jane,Doe", filePath, 3)
}

func TestDelimiterNoHeader(t *testing.T) {
	filePath := path.Join(sshDirectory, "test.csv")
	testCase := scenario{
		Name: "delimiter include header conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            false,
			MessageDelimiter:       "\n",
			IncludeHeader:          false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	GenerateTestFile(filePath, "First Name,Last Name\nJohn,Smith\nJane,Doe")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	ValidateMessage(t, proc, "John,Smith", filePath, 1)
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Jane,Doe", filePath, 2)
}

func TestDelimiterHeaderWatcher(t *testing.T) {
	filePath := path.Join(sshDirectory, "watcher_test.csv")

	testCase := scenario{
		Name: "delimiter include header conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            true,
			MessageDelimiter:       "\n",
			IncludeHeader:          true,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	defer DeleteTestFile(filePath)
	GenerateTestFile(filePath, "")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	UpdateTestFile(filePath, "First Name,Last Name\n")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "First Name,Last Name", filePath, 1)
	time.Sleep(time.Second * 1)
	UpdateTestFile(filePath, "First Name,Last Name\nJohn,Smith\n")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "John,Smith", filePath, 2)
	time.Sleep(time.Second * 1)
	UpdateTestFile(filePath, "First Name,Last Name\nJohn,Smith\nJane,Doe")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Jane,Doe", filePath, 3)
}

func TestDelimiterNoHeaderWatcher(t *testing.T) {
	filePath := path.Join(sshDirectory, "watcher_test.csv")

	testCase := scenario{
		Name: "delimiter include header conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            true,
			MessageDelimiter:       "\n",
			IncludeHeader:          false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
			ProcessExistingRecords: true,
		},
	}

	defer DeleteTestFile(filePath)
	GenerateTestFile(filePath, "")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	UpdateTestFile(filePath, "First Name,Last Name\n")
	time.Sleep(time.Second * 1)
	UpdateTestFile(filePath, "First Name,Last Name\nJohn,Smith\n")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "John,Smith", filePath, 1)
	time.Sleep(time.Second * 1)
	UpdateTestFile(filePath, "First Name,Last Name\nJohn,Smith\nJane,Doe")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Jane,Doe", filePath, 2)
}

func TestProcessDirectory(t *testing.T) {
	dirPath := path.Join(sshDirectory, t.Name())
	GenerateTestDirectory(dirPath)
	defer DeleteTestDirectory(dirPath)

	file1Path := path.Join(dirPath, "dir_process_test1.txt")
	file2Path := path.Join(dirPath, "dir_process_test2.txt")
	defer DeleteTestFile(file1Path)
	defer DeleteTestFile(file2Path)

	testCase := scenario{
		Name: "process directory",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: "",
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            false,
			MessageDelimiter:       "\n",
			IncludeHeader:          true,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          dirPath,
			DirectoryMode:          true,
			ProcessExistingRecords: true,
		},
	}

	GenerateTestFile(file1Path, "This is a test\nAnother test line")
	GenerateTestFile(file2Path, "This is the other test file\nSecond line of second file")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 5)
	assert.True(t, proc.Connected())
	ValidateMessage(t, proc, "This is a test", file1Path, 1)
	ValidateMessage(t, proc, "Another test line", file1Path, 2)

	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "This is the other test file", file2Path, 1)
	ValidateMessage(t, proc, "Second line of second file", file2Path, 2)
}

func TestDirectoryWatcherMode(t *testing.T) {
	dirPath := path.Join(sshDirectory, t.Name())
	GenerateTestDirectory(dirPath)
	defer DeleteTestDirectory(dirPath)

	filePath := path.Join(dirPath, "dir_watcher_test.txt")
	defer DeleteTestFile(filePath)

	testCase := scenario{
		Name: "watcher mode conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: "",
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            true,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          dirPath,
			DirectoryMode:          true,
			ProcessExistingRecords: true,
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())

	time.Sleep(time.Second * 5)
	assert.True(t, proc.Connected())
	GenerateTestFile(filePath, "This is a test\n")
	time.Sleep(time.Second * 1)

	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	ValidateMessage(t, proc, "This is a test\n", filePath, 1)

	// Alter the file and validate that the new line was received
	UpdateTestFile(filePath, "This is a test\nTest second line")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Test second line", filePath, 2)
}

func TestProcessExistingRecordsFalse(t *testing.T) {
	filePath := path.Join(sshDirectory, "watcher_test.txt")
	testCase := scenario{
		Name: "process existing records false",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: filePath,
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			ProcessExistingRecords: false,
			WatcherMode:            true,
			MessageDelimiter:       "\n",
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          "",
			DirectoryMode:          false,
		},
	}

	defer DeleteTestFile(filePath)

	GenerateTestFile(filePath, "This is a test\nAnother test line")
	time.Sleep(time.Second * 3)

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())

	time.Sleep(time.Second * 3)
	assert.True(t, proc.Connected())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	// Alter the file and validate that the new line was received
	UpdateTestFile(filePath, "This is a test\nAnother test line\nTest third line")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Test third line", filePath, 3)
}

func TestDirectoryProcessExistingRecordsFalse(t *testing.T) {
	dirPath := path.Join(sshDirectory, t.Name())
	GenerateTestDirectory(dirPath)
	defer DeleteTestDirectory(dirPath)

	filePath := path.Join(dirPath, "dir_watcher_test.txt")
	filePath2 := path.Join(dirPath, "dir_watcher_test2.txt")
	defer DeleteTestFile(filePath)
	defer DeleteTestFile(filePath2)

	testCase := scenario{
		Name: "directory processing existing records false",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sshPort,
			Filepath: "",
			Credentials: SFTPCredentials{
				Username: sshUsername,
				Secret:   sshPassword,
			},
			WatcherMode:            true,
			ProcessExistingRecords: false,
			MaxConnectionAttempts:  10,
			FileCheckMaxAttempts:   10,
			FileCheckSleepDuration: 1,
			DirectoryPath:          dirPath,
			DirectoryMode:          true,
			MessageDelimiter:       "\n",
		},
	}

	time.Sleep(time.Second * 1)
	GenerateTestFile(filePath, "This is a test")
	GenerateTestFile(filePath2, "This is a another test\nSecond line\nThird line")
	time.Sleep(time.Second * 1)

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	time.Sleep(time.Second * 5)
	assert.True(t, proc.Connected())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	UpdateTestFile(filePath, "This is a test\nAlso a test\nThird line test")
	time.Sleep(time.Second * 1)

	ValidateMessage(t, proc, "Also a test", filePath, 2)
	ValidateMessage(t, proc, "Third line test", filePath, 3)

	UpdateTestFile(filePath2, "This is a another test\nSecond line\nThird line\nFourth line")
	time.Sleep(time.Second * 1)
	ValidateMessage(t, proc, "Fourth line", filePath2, 4)
}

func ValidateMessage(t *testing.T, input Type, expectedMessage string, expectedFilePath string, expectedLineNum int) {
	message := <-input.TransactionChan()
	assert.NotNil(t, message, "should return non-nil message")
	assert.True(t, message.Payload.Len() > 0, "Message payload length should be greater than 0")

	part := message.Payload.Get(0)
	partBytes := part.Get()
	partStr := string(partBytes)
	assert.Equal(t, expectedMessage, partStr)
	assert.Equal(t, strconv.Itoa(expectedLineNum), part.Metadata().Get("line_num"))
	assert.Equal(t, expectedFilePath, part.Metadata().Get("file_path"))
	assert.NotEqual(t, "", part.Metadata().Get("date_created"))
	message.ResponseChan <- TestResponse{}
}

func ConnectToSSHServer(server string, port int) bool {
	// create sftp client and establish connection
	s := &SSHServer{
		Host: server,
		Port: port,
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: hostAuthCallback(),
		IsRevoked:       certCallback(s),
		HostKeyFallback: hostCallback(s),
	}

	addr := fmt.Sprintf("%s:%d", server, port)
	config := &ssh.ClientConfig{
		User: sshUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(sshPassword),
		},
		HostKeyCallback: certCheck.CheckHostKey,
	}

	var conn *ssh.Client
	var err error

	conn, err = ssh.Dial("tcp", addr, config)
	if err != nil {
		return false
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return false
	}
	sshClient = client

	return true
}

func GenerateTestFile(filepath string, data string) {
	file, err := sshClient.Create(filepath)
	if err != nil {
		log.Fatalf("Error creating file %s on SSH server", filepath)
		return
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		log.Fatalf("Error writing to file %s on SSH server", filepath)
	}
}

func UpdateTestFile(filepath string, data string) {
	file, err := sshClient.OpenFile(filepath, os.O_RDWR)
	if err != nil {
		log.Printf("Error updating file %s on SSH server", filepath)
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		log.Printf("Error writing to file %s on SSH server", filepath)
	}
}

func DeleteTestFile(filepath string) {
	err := sshClient.Remove(filepath)
	if err != nil {
		log.Printf("Error deleting file %s on SSH server", filepath)
	}
}

func GenerateTestDirectory(dirPath string) {
	err := sshClient.Mkdir(dirPath)
	if err != nil {
		log.Printf("Error creating directory %s on SSH server", dirPath)
	}
}

func DeleteTestDirectory(dirPath string) {
	err := sshClient.RemoveDirectory(dirPath)
	if err != nil {
		log.Printf("Error removing directory %s on SSH server", dirPath)
	}
}

type TestResponse struct {
}

func (TestResponse) Error() error {
	return nil
}

func (TestResponse) SkipAck() bool {
	return true
}
