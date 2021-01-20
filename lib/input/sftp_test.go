package input

import (
	"context"
	"fmt"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
	"log"
	"os"
	"path"
	"testing"
	"time"

	benthosLog "github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ssh"
)

type scenario struct {
	Name string
	Conf *SFTPConfig
}

var sftpClient *sftp.Client
var sftpUsername = "foo"
var sftpPassword = "pass"
var sftpDirectory = "/upload"
var sftpPort int

func TestMain(m *testing.M) {
	os.Exit(0)

	//pool, err := dockertest.NewPool("")
	//if err != nil {
	//	log.Fatalf("Could not connect to docker: %s", err)
	//}
	//
	//resource, err := pool.RunWithOptions(&dockertest.RunOptions{
	//	Repository: "atmoz/sftp",
	//	Tag:        "alpine",
	//	Cmd: []string{
	//		"foo:pass:1001:100:upload",
	//	},
	//})
	//
	//if err != nil {
	//	log.Fatalf("Could not start resource: %s", err)
	//}
	//
	//if err := pool.Retry(func() error {
	//	sftpPort, err = strconv.Atoi(resource.GetPort("22/tcp"))
	//	if err != nil {
	//		return err
	//	}
	//
	//	isConnected := ConnectToSFTPServer("localhost", sftpPort)
	//	if !isConnected {
	//		return errors.New("failed to connect to SSH server")
	//	}
	//
	//	return nil
	//}); err != nil {
	//	log.Fatalf("Could not connect to docker: %s", err)
	//}
	//
	//code := m.Run()
	//
	//if err := pool.Purge(resource); err != nil {
	//	log.Fatalf("Could not purge resource: %s", err)
	//}
	//
	//os.Exit(code)
}

func TestProcessFile(t *testing.T) {
	t.Skip()

	filePath := path.Join(sftpDirectory, "test.txt")
	testCase := scenario{
		Name: "good conf",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sftpPort,
			Filename: filePath,
			Credentials: SFTPCredentials{
				Username: "foo",
				Secret:   "pass",
			},
			MaxConnectionAttempts: 10,
			Path:                  "",
			Codec:                 "lines",
			DeleteObjects:         false,
		},
	}

	GenerateTestFile(filePath, "This is a test file")
	time.Sleep(time.Second * 1)

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	time.Sleep(time.Second * 3)
	err = proc.ConnectWithContext(context.Background())
	assert.NoError(t, err, "ConnectWithContext should not error")

	msg, _, err := proc.ReadWithContext(context.Background())
	assert.NoError(t, err, "ReadWithContext should not error")

	ValidateMessage(t, msg, "This is a test file", filePath)
}

func TestNoServer(t *testing.T) {
	t.Skip()

	filepath := path.Join(sftpDirectory, "test.txt")
	testCase := scenario{
		Name: "no server",
		Conf: &SFTPConfig{
			Server:   "invalid_server",
			Port:     sftpPort,
			Filename: filepath,
			Credentials: SFTPCredentials{
				Username: sftpUsername,
				Secret:   sftpPassword,
			},
			MaxConnectionAttempts: 3,
			Path:                  "",
			Codec:                 "lines",
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.Error(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")
}

func TestInvalidCredentials(t *testing.T) {
	t.Skip()

	testCase := scenario{
		Name: "invalid credentials",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sftpPort,
			Filename: path.Join(sftpDirectory, "test.txt"),
			Credentials: SFTPCredentials{
				Username: "invaliduser",
				Secret:   "invalidsecret",
			},
			MaxConnectionAttempts: 3,
			Path:                  "",
			Codec:                 "lines",
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.Error(t, err, "config should error")
	assert.NotNil(t, proc, "should return non-nil data")
}

func TestFileNotFound(t *testing.T) {
	t.Skip()

	testCase := scenario{
		Name: "file not found",
		Conf: &SFTPConfig{
			Server:   "localhost",
			Port:     sftpPort,
			Filename: path.Join(sftpDirectory, "missingfile.txt"),
			Credentials: SFTPCredentials{
				Username: sftpUsername,
				Secret:   sftpPassword,
			},
			MaxConnectionAttempts: 10,
			Path:                  "",
			Codec:                 "lines",
		},
	}

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	err = proc.ConnectWithContext(context.Background())
	assert.Error(t, err, "ConnectWithContext should error")
}

func TestProcessDirectory(t *testing.T) {
	t.Skip()

	dirPath := path.Join(sftpDirectory, t.Name())
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
			Port:     sftpPort,
			Filename: "",
			Credentials: SFTPCredentials{
				Username: sftpUsername,
				Secret:   sftpPassword,
			},
			MaxConnectionAttempts: 10,
			Path:                  dirPath,
			Codec:                 "lines",
		},
	}

	GenerateTestFile(file1Path, "This is a test\nAnother test line")
	GenerateTestFile(file2Path, "This is the other test file\nSecond line of second file")

	proc, err := NewSFTP(*testCase.Conf, benthosLog.Noop(), metrics.Noop())
	assert.NoError(t, err, "config should not error")
	assert.NotNil(t, proc, "should return non-nil data")

	err = proc.ConnectWithContext(context.Background())
	assert.NoError(t, err, "ConnectWithContext should not error")

	msg, _, err := proc.ReadWithContext(context.Background())
	assert.NoError(t, err, "ReadWithContext should not error")
	ValidateMessage(t, msg, "This is a test", file1Path)

	msg, _, err = proc.ReadWithContext(context.Background())
	assert.NoError(t, err, "ReadWithContext should not error")
	ValidateMessage(t, msg, "Another test line", file1Path)

	msg, _, err = proc.ReadWithContext(context.Background())
	assert.NoError(t, err, "ReadWithContext should not error")
	ValidateMessage(t, msg, "This is the other test file", file2Path)

	msg, _, err = proc.ReadWithContext(context.Background())
	assert.NoError(t, err, "ReadWithContext should not error")
	ValidateMessage(t, msg, "Second line of second file", file2Path)
}

func ValidateMessage(t *testing.T, msg types.Message, expectedMessage string, expectedFilePath string) {
	assert.NotNil(t, msg, "message should be non-nil")

	part := msg.Get(0)
	messageString := string(part.Get())
	assert.Equal(t, expectedMessage, messageString)
	assert.Equal(t, expectedFilePath, part.Metadata().Get("sftp_file_path"))
}

func ConnectToSFTPServer(server string, port int) bool {
	// create sftp client and establish connection
	s := &SFTPServer{
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
		User: sftpUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(sftpPassword),
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
	sftpClient = client

	return true
}

func GenerateTestFile(filepath string, data string) {
	file, err := sftpClient.Create(filepath)
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
	file, err := sftpClient.OpenFile(filepath, os.O_RDWR)
	if err != nil {
		log.Printf("Error updating file %s on SSH server", filepath)
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		log.Printf("Error writing to file %s on SSH server", filepath)
	}
}

func DeleteTestFile(filepath string) {
	err := sftpClient.Remove(filepath)
	if err != nil {
		log.Printf("Error deleting file %s on SSH server", filepath)
	}
}

func GenerateTestDirectory(dirPath string) {
	err := sftpClient.Mkdir(dirPath)
	if err != nil {
		log.Printf("Error creating directory %s on SSH server", dirPath)
	}
}

func DeleteTestDirectory(dirPath string) {
	err := sftpClient.RemoveDirectory(dirPath)
	if err != nil {
		log.Printf("Error removing directory %s on SSH server", dirPath)
	}
}