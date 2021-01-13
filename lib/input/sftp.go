package input

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/machinebox/progress"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func init() {
	var credentialsFields = docs.FieldSpecs{
		docs.FieldCommon("username", "The username to connect to the SFTP server."),
		docs.FieldCommon("secret", "The secret/password for the username to connect to the SFTP server."),
	}

	Constructors[TypeSFTP] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := NewSFTP(conf.SFTP, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeSFTP,
				true,
				reader.NewAsyncBundleUnacks(
					reader.NewAsyncPreserver(r),
				),
				log, stats,
			)
		},
		Status: docs.StatusExperimental,
		Summary: `
Downloads objects via an SFTP connection.`,
		Description: `
Downloads objects via an SFTP connection.
## Metadata
This input adds the following metadata fields to each message:
` + "```" + `
- date_created
- file_path
- line_num
- All user defined metadata
` + "```" + `
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,

		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"server",
				"The server to connect to that has the target files.",
			),
			docs.FieldCommon(
				"port",
				"The port to connect to on the server.",
			),
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the SFTP server.",
			).WithChildren(credentialsFields...),
			docs.FieldCommon(
				"filepath",
				"The path of the file to pull messages from. Filepath is only used if directory_mode is set to false.",
			),
			docs.FieldCommon(
				"watcher_mode",
				"If true, will watch for changes on the file(s) and send messages as updates are added to the file(s). " +
					       "Otherwise, it will exit after processing the file(s)",
			),
			docs.FieldCommon(
				"process_existing_records",
				"If true, the entire file(s) will be processed. " +
					"If false, only the changes made after the Benthos pipeline has been running will be processed." +
					"Only used if watcher_mode is set to true.",
			),
			docs.FieldCommon(
				"include_header",
				"Whether the first line in the file(s) is sent as a message.",
			),
			docs.FieldCommon(
				"message_delimiter",
				"The delimiter that separates messages, defaults to new line.",
			),
			docs.FieldCommon(
				"max_connection_attempts",
				"Number of times it will try to connect to the server before exiting.",
			),
			docs.FieldCommon(
				"file_check_sleep_duration",
				"How long it will sleep after failing to connect to the file or directory.",
			),
			docs.FieldCommon(
				"file_check_max_attempts",
				"Number of times it will try opening the file or directory before exiting.",
			),
			docs.FieldCommon(
				"directory_mode",
				"Whether it is processing a directory of files or a single file.",
			),
			docs.FieldCommon(
				"directory_path",
				"The path of the directory that it will process. This field is only used if directory_mode is set to true.",
			),
		},
		Categories: []Category{
			CategoryServices,
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------


// SFTPConfig contains configuration fields for the SFTP input type.
type SFTPConfig struct {
	Server                 string          `json:"server" yaml:"server"`
	Port                   int             `json:"port" yaml:"port"`
	Filepath               string          `json:"filepath" yaml:"filepath"`
	Credentials            SFTPCredentials `json:"credentials" yaml:"credentials"`
	WatcherMode            bool            `json:"watcher_mode" yaml:"watcher_mode"`
	ProcessExistingRecords bool            `json:"process_existing_records" yaml:"process_existing_records"`
	IncludeHeader          bool            `json:"include_header" yaml:"include_header"`
	MessageDelimiter       string          `json:"message_delimiter" yaml:"message_delimiter"`
	MaxConnectionAttempts  int             `json:"max_connection_attempts" yaml:"max_connection_attempts"`
	FileCheckSleepDuration int             `json:"file_check_sleep_duration" yaml:"file_check_sleep_duration"`
	FileCheckMaxAttempts   int             `json:"file_check_max_attempts" yaml:"file_check_max_attempts"`
	DirectoryPath          string          `json:"directory_path" yaml:"directory_path"`
	DirectoryMode          bool            `json:"directory_mode" yaml:"directory_mode"`
}

type SFTPCredentials struct {
	Username string `json:"username" yaml:"username"`
	Secret   string `json:"secret" yaml:"secret"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Server:                 "",
		Port:                   0,
		Filepath:               "",
		Credentials:            SFTPCredentials{},
		WatcherMode:            false,
		ProcessExistingRecords: true,
		IncludeHeader:          false,
		MessageDelimiter:       "",
		MaxConnectionAttempts:  10,
		FileCheckSleepDuration: 1,
		FileCheckMaxAttempts:   10,
		DirectoryPath:          "",
		DirectoryMode:          false,
	}
}

//------------------------------------------------------------------------------

type SFTP struct {
	server                 string
	port                   int
	filepath               string
	credentials            SFTPCredentials
	watcherMode            bool
	processExistingRecords bool
	includeHeader          bool
	messageDelimiter       string
	maxConnectionAttempts  int
	fileCheckSleepDuration int
	fileCheckMaxAttempts   int
	directoryPath          string
	directoryMode          bool

	transactionsChan chan types.Transaction

	log   log.Modular
	stats metrics.Type

	connected bool

	startTime time.Time

	bytesProcessed      int64
	totalBytesToProcess int64

	closeOnce  sync.Once
	closeChan  chan struct{}
	closedChan chan struct{}
}

func NewSFTP(conf SFTPConfig, log log.Modular, stats metrics.Type) (*SFTP, error) {
	e := &SFTP{
		server:                 conf.Server,
		port:                   conf.Port,
		filepath:               conf.Filepath,
		credentials:            conf.Credentials,
		watcherMode:            conf.WatcherMode,
		processExistingRecords: conf.ProcessExistingRecords,
		includeHeader:          conf.IncludeHeader,
		messageDelimiter:       conf.MessageDelimiter,
		maxConnectionAttempts:  conf.MaxConnectionAttempts,
		fileCheckSleepDuration: conf.FileCheckSleepDuration,
		fileCheckMaxAttempts:   conf.FileCheckMaxAttempts,
		directoryPath:          conf.DirectoryPath,
		directoryMode:          conf.DirectoryMode,

		log:   log,
		stats: stats,

		connected:           false,
		bytesProcessed:      0,
		totalBytesToProcess: 0,
		startTime:           time.Now(),

		transactionsChan: make(chan types.Transaction),
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	if conf.WatcherMode && conf.DirectoryMode {
		go e.watchDirectory()
	} else if !conf.WatcherMode && conf.DirectoryMode {
		go e.processDirectory()
	} else if conf.WatcherMode && !conf.DirectoryMode {
		go e.watchForChanges()
	} else {
		go e.processAndClose()
	}

	return e, nil
}

// TODO
func (e *SFTP) ConnectWithContext(ctx context.Context) error {
	return nil
}

// TODO
func (e *SFTP) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	return nil, nil, nil
}

func (e *SFTP) processAndClose() {
	defer func() {
		close(e.transactionsChan)
		close(e.closedChan)
	}()

	resChan := make(chan types.Response)

	_, _, client := e.initSFTPConnection()

	defer client.Close()

	bytesProcessedGauge := e.stats.GetGauge("bytes_processed")
	bytesProcessedGauge.Set(0)

	percentCompleteGauge := e.stats.GetGauge("percent_complete")
	percentCompleteGauge.Set(0)

	e.processFile(e.filepath, client, resChan)
}

func (e *SFTP) watchForChanges() {
	defer func() {
		close(e.transactionsChan)
		close(e.closedChan)
	}()

	_, _, client := e.initSFTPConnection()
	resChan := make(chan types.Response)

	defer client.Close()

	fileData := &FileData{
		Path:    e.filepath,
		Cursor:  int64(0),
		Size:    int64(0),
		LineNum: uint64(1),
	}

	e.connected = true

	for {
		e.processWatchedFile(fileData, e.filepath, client, resChan)
	}
}

func (e *SFTP) processDirectory() {
	defer func() {
		close(e.transactionsChan)
		close(e.closedChan)
	}()

	_, _, client := e.initSFTPConnection()
	resChan := make(chan types.Response)

	defer client.Close()

	e.connected = true

	files, err := client.ReadDir(e.directoryPath)
	if err != nil {
		return
	}

	totalBytes := int64(0)
	for _, f := range files {
		totalBytes += f.Size()
	}

	// Set initial values for stats gauges
	totalBytesGauge := e.stats.GetGauge("total_bytes")
	totalBytesGauge.Set(totalBytes)
	e.totalBytesToProcess = totalBytes

	bytesProcessedGauge := e.stats.GetGauge("bytes_processed")
	bytesProcessedGauge.Set(0)
	e.bytesProcessed = 0

	percentCompleteGauge := e.stats.GetGauge("percent_complete")
	percentCompleteGauge.Set(0)

	for _, f := range files {
		filePath := path.Join(e.directoryPath, f.Name())
		e.processFile(filePath, client, resChan)
	}
}

func (e *SFTP) watchDirectory() {
	defer func() {
		close(e.transactionsChan)
		close(e.closedChan)
	}()

	_, _, client := e.initSFTPConnection()
	resChan := make(chan types.Response)

	defer client.Close()

	fileData := map[string]*FileData{}

	e.connected = true

	for {
		files, err := client.ReadDir(e.directoryPath)
		if err != nil {
			e.log.Errorf("Error reading directory: %s", e.directoryPath)
			continue
		}

		for _, f := range files {
			data, ok := fileData[f.Name()]
			if ok {
				if data.Size == f.Size() {
					continue
				}
			} else {
				data = &FileData{
					Path:    f.Name(),
					Cursor:  int64(0),
					Size:    int64(0),
					LineNum: uint64(1),
				}
				fileData[f.Name()] = data
			}
			filePath := path.Join(e.directoryPath, f.Name())

			e.processWatchedFile(data, filePath, client, resChan)
		}
	}
}

func (e *SFTP) processFile(filePath string, client *sftp.Client, resChan chan types.Response) {
	// read from file
	fileErrorCounter := e.stats.GetCounter("file_does_not_exist_errors")
	info, err := client.Stat(filePath)
	fileCheckAttempts := 0
	for err != nil {
		fileCheckAttempts++
		fileErrorCounter.Incr(1)
		e.log.Errorf("File %s does not exist on server", filePath)
		time.Sleep(time.Second * time.Duration(e.fileCheckSleepDuration))
		info, err = client.Stat(filePath)

		if fileCheckAttempts >= e.fileCheckMaxAttempts {
			e.log.Errorf("Exiting after %i attempts to read file %s", fileCheckAttempts, filePath)
		}
	}

	e.connected = true

	fileSize := info.Size()

	// Only set the total bytes to the file size if it's not in directory mode
	if !e.directoryMode {
		totalBytesGauge := e.stats.GetGauge("total_bytes")
		totalBytesGauge.Set(fileSize)
		e.totalBytesToProcess = fileSize
	}

	file, err := client.Open(filePath)
	if err != nil {
		e.log.Errorf("Error opening file %s: %s", filePath, err.Error())
		return
	}

	reader := progress.NewReader(file)

	bytesProcessedGauge := e.stats.GetGauge("bytes_processed")
	percentCompleteGauge := e.stats.GetGauge("percent_complete")

	go func() {
		ctx := context.Background()
		progressChan := progress.NewTicker(ctx, reader, fileSize, 1*time.Second)

		previousBytes := int64(0)
		for p := range progressChan {
			bytesProcessed := p.N()
			newBytesProcessed := bytesProcessed - previousBytes
			bytesProcessedGauge.Incr(newBytesProcessed)
			e.bytesProcessed += newBytesProcessed

			percent := (float64(e.bytesProcessed) / float64(e.totalBytesToProcess)) * 100
			percentCompleteGauge.Set(int64(percent))
			previousBytes = bytesProcessed
		}
	}()

	b, _ := ioutil.ReadAll(reader)
	var messageBytes [][]byte

	if e.messageDelimiter != "" {
		fileString := string(b)
		messages := strings.Split(fileString, e.messageDelimiter)
		for i, m := range messages {
			if i == 0 && !e.includeHeader {
				continue
			}
			if strings.TrimSpace(m) != "" {
				messageBytes = append(messageBytes, []byte(m))
			}
		}
	} else {
		messageBytes = append(messageBytes, b)
	}

	for i, m := range messageBytes {
		benthosMessage := e.generateMessage(m, uint64(i+1), filePath)

		// send batch to downstream processors
		select {
		case e.transactionsChan <- types.NewTransaction(
			benthosMessage,
			resChan,
		):
		case <-e.closeChan:
			return
		}

		// check transaction success
		select {
		case result := <-resChan:
			if nil != result.Error() {
				e.log.Errorln(result.Error().Error())
				return
			}
		case <-e.closeChan:
			return
		}
	}
}

func (e *SFTP) processWatchedFile(fileData *FileData, filePath string, client *sftp.Client, resChan chan types.Response) {
	fileInfo, err := client.Stat(filePath)
	if err != nil {
		e.log.Errorf("Error getting file info for file %s: %s", filePath, err.Error())
		return
	}

	newFileSize := fileInfo.Size()
	if fileData.Size == newFileSize {
		return
	}

	file, err := client.Open(filePath)
	if err != nil {
		e.log.Errorf("Error opening file %s: %s", filePath, err.Error())
		return
	}

	_, err = file.Seek(fileData.Cursor, io.SeekStart)
	if err != nil {
		e.log.Errorf("Error calling file. Seek: %s", err.Error())
		return
	}

	length := fileInfo.Size() - fileData.Cursor
	if length <= 0 {
		return
	}

	if fileInfo.ModTime().Before(e.startTime) && !e.processExistingRecords && fileData.Size != newFileSize {
		fileData.Cursor = newFileSize
		if e.messageDelimiter != "" {
			bytes := make([]byte, length)
			_, err = file.Read(bytes)
			fileString := string(bytes)
			lines := strings.Split(fileString, e.messageDelimiter)

			fileData.LineNum = uint64(len(lines) + 1)
		}
		return
	}

	bytes := make([]byte, length)
	_, err = file.Read(bytes)

	var messageBytes [][]byte

	if e.messageDelimiter != "" {
		fileString := string(bytes)
		messages := strings.Split(fileString, e.messageDelimiter)
		for i, m := range messages {
			if fileData.Cursor == 0 && i == 0 && !e.includeHeader {
				continue
			}
			if strings.TrimSpace(m) != "" {
				messageBytes = append(messageBytes, []byte(m))
			}
		}
	} else {
		messageBytes = append(messageBytes, bytes)
	}

	for _, m := range messageBytes {
		benthosMessage := e.generateMessage(m, fileData.LineNum, filePath)
		fileData.LineNum++

		// send batch to downstream processors
		select {
		case e.transactionsChan <- types.NewTransaction(
			benthosMessage,
			resChan,
		):
		case <-e.closeChan:
			return
		}

		// check transaction success
		select {
		case result := <-resChan:
			if nil != result.Error() {
				e.log.Errorln(result.Error().Error())
				return
			}
		case <-e.closeChan:
			return
		}
	}

	fileData.Cursor = newFileSize
	fileData.Size = newFileSize
}

func (e *SFTP) initSFTPConnection() (*SSHServer, *ssh.CertChecker, *sftp.Client) {
	// create sftp client and establish connection
	s := &SSHServer{
		Host: e.server,
		Port: e.port,
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: hostAuthCallback(),
		IsRevoked:       certCallback(s),
		HostKeyFallback: hostCallback(s),
	}

	addr := fmt.Sprintf("%s:%d", e.server, e.port)
	config := &ssh.ClientConfig{
		User: e.credentials.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(e.credentials.Secret),
		},
		HostKeyCallback: certCheck.CheckHostKey,
	}

	var conn *ssh.Client
	var err error
	connectionAttempts := 0
	for {
		connectionAttempts++
		conn, err = ssh.Dial("tcp", addr, config)
		if err != nil {
			connectionErrorsCounter := e.stats.GetCounter("connection_errors")
			connectionErrorsCounter.Incr(1)
			e.log.Errorf("Failed to dial: %s", err.Error())
			if connectionAttempts >= e.maxConnectionAttempts {
				e.log.Errorf("Failed to connect after %i attempts, stopping", connectionAttempts)
			}
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

	client, err := sftp.NewClient(conn)

	if err != nil {
		clientErrorsCounter := e.stats.GetCounter("client_errors")
		clientErrorsCounter.Incr(1)
		e.log.Errorf("Failed to create client: %s", err.Error())
		time.Sleep(time.Second * 5)
		e.initSFTPConnection()
	}

	return s, certCheck, client
}

func (e *SFTP) generateMessage(messageBytes []byte, lineNum uint64, filePath string) *message.Type {
	benthosMessage := message.New([][]byte{messageBytes})
	part := benthosMessage.Get(0)
	metadata := part.Metadata()
	metadata.Set("date_created", time.Now().String())
	metadata.Set("file_path", filePath)
	metadata.Set("line_num", strconv.Itoa(int(lineNum)))
	return benthosMessage
}

type SSHServer struct {
	Address   string          // host:port
	Host      string          // IP address
	Port      int             // port
	IsSSH     bool            // true if server is running SSH on address:port
	Banner    string          // banner text, if any
	Cert      ssh.Certificate // server's certificate
	Hostname  string          // hostname
	PublicKey ssh.PublicKey   // server's public key
}

type HostAuthorityCallBack func(ssh.PublicKey, string) bool
type IsRevokedCallback func(cert *ssh.Certificate) bool

func hostAuthCallback() HostAuthorityCallBack {
	return func(p ssh.PublicKey, addr string) bool {
		return true
	}
}

func certCallback(s *SSHServer) IsRevokedCallback {
	return func(cert *ssh.Certificate) bool {
		s.Cert = *cert
		s.IsSSH = true

		return false
	}
}

func hostCallback(s *SSHServer) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}

func (e *SFTP) Connected() bool {
	return e.connected
}

func (e *SFTP) TransactionChan() <-chan types.Transaction {
	return e.transactionsChan
}

func (e *SFTP) CloseAsync() {
	e.closeOnce.Do(func() {
		close(e.closeChan)
	})
}

func (e *SFTP) WaitForClose(timeout time.Duration) error {
	select {
	case <-e.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

type FileData struct {
	Path    string
	Cursor  int64
	Size    int64
	LineNum uint64
}
