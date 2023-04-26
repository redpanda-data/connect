package studio

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
)

func pullCommand(version, dateBuilt string) *cli.Command {
	return &cli.Command{
		Name:  "pull",
		Usage: "Run deployments configured within a Benthos Studio session",
		Description: `
When a Studio session has one or more deployments added this command will
synchronise with the session and obtain a deployment assignment. The assigned
deployment will then determine which configs from the session to download and
execute.

When either changes are made to files of an assigned deployment, or when a new
deployment is assigned, this service will automatically download the new config
files and execute them, replacing the previous stream running.`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "session",
				Aliases:  []string{"s"},
				Required: true,
				Value:    "",
				Usage:    "The session ID to synchronise with.",
			},
			&cli.StringFlag{
				Name:  "name",
				Value: "",
				Usage: "An explicit name to adopt in this instance, used to identify its connection to the session. Each running node must have a unique name, if left unset a name is generated each time the command is run.",
			},
			&cli.StringFlag{
				Name:  "token",
				Value: "",
				Usage: "A token for the session, used to authenticate requests. If left blank the environment variable BSTDIO_NODE_TOKEN will be used instead.",
			},
			&cli.StringFlag{
				Name:  "token-secret",
				Value: "",
				Usage: "A token secret the session, used to authenticate requests. If left blank the environment variable BSTDIO_NODE_SECRET will be used instead.",
			},
			&cli.BoolFlag{
				Name:  "send-traces",
				Value: false,
				Usage: "Whether to send trace data back to Studio during execution. This is opt-in and is used as a way to add trace events to the graph editor for testing and debugging configs. This is a very useful feature but should be used with caution as it exports information about messages passing through the stream.",
			},
		},
		Action: func(c *cli.Context) error {
			// Start off by warning about all unsupported flags
			if c.Bool("watcher") {
				fmt.Fprintln(os.Stderr, "The --watcher/-w flag is not supported in this mode of operation")
				os.Exit(1)
			}

			token, secret := c.String("token"), c.String("token-secret")
			if token == "" {
				if token = os.Getenv("BSTDIO_NODE_TOKEN"); token == "" {
					fmt.Fprintln(os.Stderr, "Must specify either --token or BSTDIO_NODE_TOKEN")
				}
			}
			if secret == "" {
				if secret = os.Getenv("BSTDIO_NODE_SECRET"); secret == "" {
					fmt.Fprintln(os.Stderr, "Must specify either --token-secret or BSTDIO_NODE_SECRET")
				}
			}
			if token == "" || secret == "" {
				os.Exit(1)
			}

			pullRunner, err := NewPullRunner(c, version, dateBuilt, token, secret)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error encountered whilst initiating studio sync: %v\n", err)
				os.Exit(1)
			}

			sigCtx, done := context.WithCancel(context.Background())
			defer done()

			// TODO: Replace this with context.WithCancelCause once 1.20 is our
			// minimum version.
			var sigName string
			var sigNameMut sync.Mutex

			go func() {
				sigChan := make(chan os.Signal, 1)
				signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
				select {
				case sig := <-sigChan:
					sigNameMut.Lock()
					switch sig {
					case os.Interrupt:
						sigName = "SIGINT"
					case syscall.SIGTERM:
						sigName = "SIGTERM"
					default:
						sigName = sig.String()
					}
					sigNameMut.Unlock()
					done()
				case <-sigCtx.Done():
					return
				}
			}()

			syncTicker := time.NewTicker(time.Second * 5)
			defer syncTicker.Stop()
			for {
				// Wait for either a termination signal, or the next sync timer
				select {
				case <-syncTicker.C:
					pullRunner.Sync(sigCtx)
				case <-sigCtx.Done():
					sigNameMut.Lock()
					pullRunner.logger.Infof("Received signal %s, shutting down", sigName)
					sigNameMut.Unlock()
					if pullRunner.Stop(context.Background()) != nil {
						os.Exit(1)
					}
					return nil
				}
			}
		},
	}
}
