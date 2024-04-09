package blobl

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Jeffail/gabs/v2"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/value"
)

var red = color.New(color.FgRed).SprintFunc()

// CliCommand is a cli.Command definition for running a blobl mapping.
func CliCommand() *cli.Command {
	return &cli.Command{
		Name:  "blobl",
		Usage: "Execute a Bloblang mapping on documents consumed via stdin",
		Description: `
Provides a convenient tool for mapping JSON documents over the command line:

  cat documents.jsonl | benthos blobl 'foo.bar.map_each(this.uppercase())'

  echo '{"foo":"bar"}' | benthos blobl -f ./mapping.blobl

Find out more about Bloblang at: https://benthos.dev/docs/guides/bloblang/about`[1:],
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "threads",
				Aliases: []string{"t"},
				Value:   1,
				Usage:   "the number of processing threads to use, when >1 ordering is no longer guaranteed.",
			},
			&cli.BoolFlag{
				Name:    "raw",
				Aliases: []string{"r"},
				Usage:   "consume raw strings.",
			},
			&cli.BoolFlag{
				Name:    "pretty",
				Aliases: []string{"p"},
				Usage:   "pretty-print output.",
			},
			&cli.StringFlag{
				Name:    "file",
				Aliases: []string{"f"},
				Usage:   "execute a mapping from a file.",
			},
			&cli.IntFlag{
				Name:  "max-token-length",
				Usage: "Set the buffer size for document lines.",
				Value: bufio.MaxScanTokenSize,
			},
		},
		Action: run,
		Subcommands: []*cli.Command{
			{
				Name:        "server",
				Usage:       "EXPERIMENTAL: Run a web server that hosts a Bloblang app",
				Description: "Run a web server that provides an interactive application for writing and testing Bloblang mappings.",
				Action:      runServer,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "host",
						Value: "localhost",
						Usage: "the host to bind to.",
					},
					&cli.StringFlag{
						Name:    "port",
						Value:   "4195",
						Aliases: []string{"p"},
						Usage:   "the port to bind to.",
					},
					&cli.BoolFlag{
						Name:    "no-open",
						Value:   false,
						Aliases: []string{"n"},
						Usage:   "do not open the app in the browser automatically.",
					},
					&cli.StringFlag{
						Name:    "mapping-file",
						Value:   "",
						Aliases: []string{"m"},
						Usage:   "an optional path to a mapping file to load as the initial mapping within the app.",
					},
					&cli.StringFlag{
						Name:    "input-file",
						Value:   "",
						Aliases: []string{"i"},
						Usage:   "an optional path to an input file to load as the initial input to the mapping within the app.",
					},
					&cli.BoolFlag{
						Name:    "write",
						Value:   false,
						Aliases: []string{"w"},
						Usage:   "when editing a mapping and/or input file write changes made back to the respective source file, if the file does not exist it will be created.",
					},
				},
			},
		},
	}
}

type execCache struct {
	msg  message.Batch
	vars map[string]any
}

func newExecCache() *execCache {
	return &execCache{
		msg:  message.QuickBatch([][]byte{[]byte(nil)}),
		vars: map[string]any{},
	}
}

func (e *execCache) executeMapping(exec *mapping.Executor, rawInput, prettyOutput bool, input []byte) (string, error) {
	e.msg.Get(0).SetBytes(input)

	var valuePtr *any
	var parseErr error

	lazyValue := func() *any {
		if valuePtr == nil && parseErr == nil {
			if rawInput {
				var value any = input
				valuePtr = &value
			} else {
				if jObj, err := e.msg.Get(0).AsStructured(); err == nil {
					valuePtr = &jObj
				} else {
					if errors.Is(err, message.ErrMessagePartNotExist) {
						parseErr = errors.New("message is empty")
					} else {
						parseErr = fmt.Errorf("parse as json: %w", err)
					}
				}
			}
		}
		return valuePtr
	}

	for k := range e.vars {
		delete(e.vars, k)
	}

	var result any = value.Nothing(nil)
	err := exec.ExecOnto(query.FunctionContext{
		Maps:     exec.Maps(),
		Vars:     e.vars,
		MsgBatch: e.msg,
		NewMeta:  e.msg.Get(0),
		NewValue: &result,
	}.WithValueFunc(lazyValue), mapping.AssignmentContext{
		Vars:  e.vars,
		Meta:  e.msg.Get(0),
		Value: &result,
	})
	if err != nil {
		var ctxErr query.ErrNoContext
		if parseErr != nil && errors.As(err, &ctxErr) {
			if ctxErr.FieldName != "" {
				err = fmt.Errorf("unable to reference message as structured (with 'this.%v'): %w", ctxErr.FieldName, parseErr)
			} else {
				err = fmt.Errorf("unable to reference message as structured (with 'this'): %w", parseErr)
			}
		}
		return "", err
	}

	var resultStr string
	switch t := result.(type) {
	case string:
		resultStr = t
	case []byte:
		resultStr = string(t)
	case value.Delete:
		return "", nil
	case value.Nothing:
		// Do not change the original contents
		if v := lazyValue(); v != nil {
			gObj := gabs.Wrap(v)
			if prettyOutput {
				resultStr = gObj.StringIndent("", "  ")
			} else {
				resultStr = gObj.String()
			}
		} else {
			resultStr = string(input)
		}
	default:
		gObj := gabs.Wrap(result)
		if prettyOutput {
			resultStr = gObj.StringIndent("", "  ")
		} else {
			resultStr = gObj.String()
		}
	}

	// TODO: Return metadata as well?
	return resultStr, nil
}

func run(c *cli.Context) error {
	t := c.Int("threads")
	if t < 1 {
		t = 1
	}
	raw := c.Bool("raw")
	pretty := c.Bool("pretty")
	file := c.String("file")
	m := c.Args().First()

	if file != "" {
		if m != "" {
			fmt.Fprintln(os.Stderr, red("invalid flags, unable to execute both a file mapping and an inline mapping"))
			os.Exit(1)
		}
		mappingBytes, err := ifs.ReadFile(ifs.OS(), file)
		if err != nil {
			fmt.Fprintf(os.Stderr, red("failed to read mapping file: %v\n"), err)
			os.Exit(1)
		}
		m = string(mappingBytes)
	}

	bEnv := bloblang.NewEnvironment().WithImporterRelativeToFile(file)
	exec, err := bEnv.NewMapping(m)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			fmt.Fprintf(os.Stderr, "%v %v\n", red("failed to parse mapping:"), perr.ErrorAtPositionStructured("", []rune(m)))
		} else {
			fmt.Fprintln(os.Stderr, red(err.Error()))
		}
		os.Exit(1)
	}

	inputsChan := make(chan []byte)
	go func() {
		defer close(inputsChan)

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(nil, c.Int("max-token-length"))
		for scanner.Scan() {
			input := make([]byte, len(scanner.Bytes()))
			copy(input, scanner.Bytes())
			inputsChan <- input
		}
		if scanner.Err() != nil {
			fmt.Fprintln(os.Stderr, red(scanner.Err()))
			os.Exit(1)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(t)
	resultsChan := make(chan string)
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for i := 0; i < t; i++ {
		go func() {
			defer wg.Done()

			execCache := newExecCache()
			for {
				input, open := <-inputsChan
				if !open {
					return
				}

				resultStr, err := execCache.executeMapping(exec, raw, pretty, input)
				if err != nil {
					fmt.Fprintln(os.Stderr, red(fmt.Sprintf("failed to execute map: %v", err)))
					continue
				}
				resultsChan <- resultStr
			}
		}()
	}

	for res := range resultsChan {
		fmt.Println(res)
	}
	os.Exit(0)
	return nil
}
