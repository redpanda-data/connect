package input

import (
	"context"
	"errors"
	"fmt"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"io"
	"io/ioutil"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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
				"directory_mode",
				"Whether it is processing a directory of files or a single file.",
			),
			docs.FieldCommon(
				"directory_path",
				"The path of the directory that it will process. This field is only used if directory_mode is set to true.",
			),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_objects", "Whether to delete downloaded objects from the blob once they are processed."),
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
	Server                string          `json:"server" yaml:"server"`
	Port                  int             `json:"port" yaml:"port"`
	Filepath              string          `json:"filepath" yaml:"filepath"`
	Credentials           SFTPCredentials `json:"credentials" yaml:"credentials"`
	DirectoryPath         string          `json:"directory_path" yaml:"directory_path"`
	DirectoryMode         bool            `json:"directory_mode" yaml:"directory_mode"`
	MaxConnectionAttempts int             `json:"max_connection_attempts" yaml:"max_connection_attempts"`
	Codec                 string          `json:"codec" yaml:"codec"`
	DeleteObjects         bool            `json:"delete_objects" yaml:"delete_objects"`
}

type SFTPCredentials struct {
	Username string `json:"username" yaml:"username"`
	Secret   string `json:"secret" yaml:"secret"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Server:                "",
		Port:                  0,
		Filepath:              "",
		Credentials:           SFTPCredentials{},
		MaxConnectionAttempts: 10,
		DirectoryPath:         "",
		DirectoryMode:         false,
		Codec:                 "lines",
		DeleteObjects:         false,
	}
}

//------------------------------------------------------------------------------

type SFTP struct {
	conf SFTPConfig

	log   log.Modular
	stats metrics.Type

	client *sftp.Client

	objectScannerCtor codec.ReaderConstructor
	keyReader         *sftpTargetReader

	objectMut sync.Mutex
	object    *sftpPendingObject
}

func NewSFTP(conf SFTPConfig, log log.Modular, stats metrics.Type) (*SFTP, error) {
	var err error
	var objectScannerCtor codec.ReaderConstructor
	if objectScannerCtor, err = codec.GetReader(conf.Codec, codec.NewReaderConfig()); err != nil {
		return nil, fmt.Errorf("invalid sftp codec: %v", err)
	}

	s := &SFTP{
		conf:              conf,
		log:               log,
		stats:             stats,
		objectScannerCtor: objectScannerCtor,
	}

	err = s.initSFTPConnection()

	return s, err
}

type sftpObjectTarget struct {
	key   string
	ackFn func(context.Context, error) error
}

func (s *SFTP) ConnectWithContext(ctx context.Context) error {
	var err error
	s.keyReader, err = newSFTPTargetReader(ctx, s.conf, s.log, s.client)
	return err
}

func deleteSFTPObjectAckFn(
	client *sftp.Client,
	key string,
	delete bool,
	prev codec.ReaderAckFn,
) codec.ReaderAckFn {
	return func(ctx context.Context, err error) error {
		if prev != nil {
			if aerr := prev(ctx, err); aerr != nil {
				return aerr
			}
		}
		if !delete || err != nil {
			return nil
		}

		_, err = client.Stat(key)
		if err != nil {
			return nil
		}

		aerr := client.Remove(key)

		return aerr
	}
}

//------------------------------------------------------------------------------

type sftpTargetReader struct {
	pending    []*sftpObjectTarget
	conf       SFTPConfig
	startAfter string
}

type sftpPendingObject struct {
	target    *sftpObjectTarget
	obj       *sftp.File
	extracted int
	scanner   codec.Reader
}

func newSFTPTargetReader(
	ctx context.Context,
	conf SFTPConfig,
	log log.Modular,
	client *sftp.Client,
) (*sftpTargetReader, error) {
	var files []*sftp.File

	if !conf.DirectoryMode {
		file, err := client.Open(conf.Filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %v", err)
		}
		files = append(files, file)
	} else {
		fileInfos, err := client.ReadDir(conf.DirectoryPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open directory: %v", err)
		}
		for _, fileInfo := range fileInfos {
			filepath := path.Join(conf.DirectoryPath, fileInfo.Name())
			file, err := client.Open(filepath)
			if err != nil {
				continue
			}
			files = append(files, file)
		}
	}

	staticKeys := sftpTargetReader{
		conf: conf,
	}

	for _, file := range files {
		filepath := conf.Filepath
		if conf.DirectoryMode {
			filepath = path.Join(conf.DirectoryPath, file.Name())
		}
		ackFn := deleteSFTPObjectAckFn(client, filepath, conf.DeleteObjects, nil)
		staticKeys.pending = append(staticKeys.pending, newSFTPObjectTarget(file.Name(), ackFn))
	}

	return &staticKeys, nil
}

func (s *sftpTargetReader) Pop(ctx context.Context) (*sftpObjectTarget, error) {
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	obj := s.pending[0]
	s.pending = s.pending[1:]
	return obj, nil
}

//------------------------------------------------------------------------------

func newSFTPObjectTarget(key string, ackFn codec.ReaderAckFn) *sftpObjectTarget {
	if ackFn == nil {
		ackFn = func(context.Context, error) error {
			return nil
		}
	}
	return &sftpObjectTarget{key: key, ackFn: ackFn}
}

func (s *SFTP) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	s.objectMut.Lock()
	defer s.objectMut.Unlock()

	defer func() {
		if errors.Is(err, io.EOF) {
			err = types.ErrTypeClosed
		} else if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) ||
			(err != nil && strings.HasSuffix(err.Error(), "context canceled")) {
			err = types.ErrTimeout
		}
	}()

	var object *sftpPendingObject
	if object, err = s.getObjectTarget(ctx); err != nil {
		return
	}

	var p types.Part
	var scnAckFn codec.ReaderAckFn

scanLoop:
	for {
		if p, scnAckFn, err = object.scanner.Next(ctx); err == nil {
			object.extracted++
			break scanLoop
		}
		s.object = nil
		if err != io.EOF {
			return
		}
		if err = object.scanner.Close(ctx); err != nil {
			s.log.Warnf("Failed to close sftp object scanner cleanly: %v\n", err)
		}
		if object.extracted == 0 {
			s.log.Debugf("Extracted zero messages from key %v\n", object.target.key)
		}
		if object, err = s.getObjectTarget(ctx); err != nil {
			return
		}
	}

	return sftpMsgFromPart(object, p), func(rctx context.Context, res types.Response) error {
		return scnAckFn(rctx, res.Error())
	}, nil
}

func sftpMsgFromPart(p *sftpPendingObject, part types.Part) types.Message {
	msg := message.New(nil)
	msg.Append(part)

	meta := msg.Get(0).Metadata()

	fileInfo, _ := p.obj.Stat()

	meta.Set("sftp_file_path", p.target.key)
	meta.Set("sftp_file_modification_time", fileInfo.ModTime().String())
	//meta.Set("line_num", strconv.Itoa(int(lineNum)))

	return msg
}

func (s *SFTP) initSFTPConnection() error {
	// create sftp client and establish connection
	server := &SFTPServer{
		Host: s.conf.Server,
		Port: s.conf.Port,
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: hostAuthCallback(),
		IsRevoked:       certCallback(server),
		HostKeyFallback: hostCallback(server),
	}

	addr := fmt.Sprintf("%s:%d", s.conf.Server, s.conf.Port)
	config := &ssh.ClientConfig{
		User: s.conf.Credentials.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(s.conf.Credentials.Secret),
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
			connectionErrorsCounter := s.stats.GetCounter("connection_errors")
			connectionErrorsCounter.Incr(1)
			s.log.Errorf("Failed to dial: %s", err.Error())
			if connectionAttempts >= s.conf.MaxConnectionAttempts {
				s.log.Errorf("Failed to connect after %i attempts, stopping", connectionAttempts)
				return errors.New("failed to connect to SFTP server")
			}
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

	client, err := sftp.NewClient(conn)

	if err != nil {
		clientErrorsCounter := s.stats.GetCounter("client_errors")
		clientErrorsCounter.Incr(1)
		s.log.Errorf("Failed to create client: %s", err.Error())
	}

	s.client = client

	return err
}

func (s *SFTP) getObjectTarget(ctx context.Context) (*sftpPendingObject, error) {
	if s.object != nil {
		return s.object, nil
	}

	target, err := s.keyReader.Pop(ctx)
	if err != nil {
		return nil, err
	}

	_, err = s.client.Stat(target.key)
	if err != nil {
		target.ackFn(ctx, err)
		return nil, fmt.Errorf("target file does not exist: %v", err)
	}

	file, err := s.client.Open(target.key)
	if err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	obj := ioutil.NopCloser(file)

	object := &sftpPendingObject{
		target: target,
		obj:    file,
	}

	if object.scanner, err = s.objectScannerCtor(target.key, obj, target.ackFn); err != nil {
		target.ackFn(ctx, err)
		return nil, err
	}

	s.object = object
	return object, nil
}

type SFTPServer struct {
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

func certCallback(s *SFTPServer) IsRevokedCallback {
	return func(cert *ssh.Certificate) bool {
		s.Cert = *cert
		s.IsSSH = true

		return false
	}
}

func hostCallback(s *SFTPServer) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}

func (s *SFTP) CloseAsync() {
	go func() {
		s.objectMut.Lock()
		if s.object != nil {
			s.object.scanner.Close(context.Background())
			s.object = nil
		}
		s.objectMut.Unlock()
	}()
}

func (s *SFTP) WaitForClose(timeout time.Duration) error {
	return nil
}
