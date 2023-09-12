package io

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cpNameField = "name"
	cpArgsField = "args_mapping"
)

func commandProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.21.0").
		Categories("Integration").
		Summary("Executes a command for each message.").
		Description(`
The specified command is executed for each message processed, with the raw bytes of the message being fed into the stdin of the command process, and the resulting message having its contents replaced with the stdout of it.

## Performance

Since this processor executes a new process for each message performance will likely be an issue for high throughput streams. If this is the case then consider using the [`+"`subprocess` processor"+`](/docs/components/processors/subprocess) instead as it keeps the underlying process alive long term and uses codecs to insert and extract inputs and outputs to it via stdin/stdout.

## Error Handling

If a non-zero error code is returned by the command then an error containing the entirety of stderr (or a generic message if nothing is written) is set on the message. These failed messages will continue through the pipeline unchanged, but can be dropped or placed in a dead letter queue according to your config, you can read about these patterns [here](/docs/configuration/error_handling).

If the command is successful but stderr is written to then a metadata field `+"`command_stderr`"+` is populated with its contents.
`).
		Fields(
			service.NewInterpolatedStringField(cpNameField).
				Description("The name of the command to execute.").
				Examples("bash", "go", "${! @command }"),
			service.NewBloblangField(cpArgsField).
				Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) that, when specified, should resolve into an array of arguments to pass to the command. Command arguments are expressed this way in order to support dynamic behaviour.").
				Optional().
				Examples(`[ "-c", this.script_path ]`),
		).
		Example(
			"Cron Scheduled Command",
			`This example uses a [`+"`generate`"+` input](/docs/components/inputs/generate) to trigger a command on a cron schedule:`,
			`
input:
  generate:
    interval: '0,30 */2 * * * *'
    mapping: 'root = ""' # Empty string as we do not need to pipe anything to stdin
  processors:
    - command:
        name: df
        args_mapping: '[ "-h" ]'
`,
		).
		Example(
			"Dynamic Command Execution",
			`This example config takes structured messages of the form `+"`"+`{"command":"echo","args":["foo"]}`+"`"+` and uses their contents to execute the contained command and arguments dynamically, replacing its contents with the command result printed to stdout:`,
			`
pipeline:
  processors:
    - command:
        name: ${! this.command }
        args_mapping: 'this.args'
`,
		)
}

func init() {
	err := service.RegisterProcessor(
		"command", commandProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newCommandProcFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type commandProc struct {
	name        *service.InterpolatedString
	argsMapping *bloblang.Executor
}

func newCommandProcFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (proc *commandProc, err error) {
	proc = &commandProc{}

	if proc.name, err = conf.FieldInterpolatedString(cpNameField); err != nil {
		return
	}

	if conf.Contains(cpArgsField) {
		if proc.argsMapping, err = conf.FieldBloblang(cpArgsField); err != nil {
			return
		}
	}

	return proc, nil
}

func (c *commandProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	name, err := c.name.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("name interpolation error: %w", err)
	}

	var args []string

	if c.argsMapping != nil {
		mapRes, err := msg.BloblangQuery(c.argsMapping)
		if err != nil {
			return nil, fmt.Errorf("args mapping error: %w", err)
		}

		mapResI, err := mapRes.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("args mapping error: %w", err)
		}

		switch t := mapResI.(type) {
		case []any:
			args = make([]string, len(t))
			for i, v := range t {
				args[i] = query.IToString(v)
			}
		case []string:
			args = t
		default:
			return nil, fmt.Errorf("args mapping result error: %w", query.NewTypeError(mapResI, query.ValueArray))
		}
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = bytes.NewReader(msgBytes)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	outBytes, errBytes := stdout.Bytes(), stderr.Bytes()
	if err != nil {
		return nil, fmt.Errorf("execution error: %w: %s", err, errBytes)
	}

	msg.SetBytes(outBytes)
	if len(errBytes) > 0 {
		msg.MetaSet("command_stderr", string(errBytes))
	}
	return service.MessageBatch{msg}, nil
}

func (c *commandProc) Close(ctx context.Context) error {
	return nil
}
