package blobl

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/gabs/v2"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
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

   Find out more about Bloblang at: https://benthos.dev/docs/guides/bloblang/about`[4:],
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
		},
		Action: run,
	}
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

	if len(file) > 0 {
		if len(m) > 0 {
			fmt.Fprintln(os.Stderr, red("invalid flags, unable to execute both a file mapping and an inline mapping"))
			os.Exit(1)
		}
		mappingBytes, err := ioutil.ReadFile(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, red("failed to read mapping file: %v\n"), err)
			os.Exit(1)
		}
		m = string(mappingBytes)
	}

	exec, err := bloblang.NewMapping(file, m)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			fmt.Fprintln(os.Stderr, red(fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(m)))))
		} else {
			fmt.Fprintln(os.Stderr, red(err.Error()))
		}
		os.Exit(1)
	}

	inputsChan := make(chan []byte)
	go func() {
		defer close(inputsChan)

		scanner := bufio.NewScanner(os.Stdin)
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

		inputsLoop:
			for {
				input, open := <-inputsChan
				if !open {
					return
				}

				msg := message.New([][]byte{input})

				var value interface{}
				if raw {
					value = input
				} else {
					var err error
					if value, err = msg.Get(0).JSON(); err != nil {
						fmt.Fprintln(os.Stderr, red(fmt.Sprintf("failed to parse JSON: %v", err)))
						continue
					}
				}

				result, err := exec.Exec(query.FunctionContext{
					Value:    &value,
					Maps:     map[string]query.Function{},
					Vars:     map[string]interface{}{},
					MsgBatch: msg,
				})
				if err != nil {
					fmt.Fprintln(os.Stderr, red(fmt.Sprintf("failed to execute map: %v", err)))
					continue
				}

				var resultStr string
				switch t := result.(type) {
				case string:
					resultStr = t
				case []byte:
					resultStr = string(t)
				case query.Delete:
					// Return nothing (filter the message)
					continue inputsLoop
				case query.Nothing:
					// Do not change the original contents
					gObj := gabs.Wrap(value)
					if pretty {
						resultStr = gObj.StringIndent("", "  ")
					} else {
						resultStr = gObj.String()
					}
				default:
					gObj := gabs.Wrap(result)
					if pretty {
						resultStr = gObj.StringIndent("", "  ")
					} else {
						resultStr = gObj.String()
					}
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
