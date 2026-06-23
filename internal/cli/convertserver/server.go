// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package convertserver

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/urfave/cli/v2"

	connectconverter "github.com/redpanda-data/connect/v4/internal/connect_converter"
)

//go:embed resources/page.html
var pageHTML []byte

type warningJSON struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type convertResponse struct {
	YAML     string        `json:"yaml"`
	Warnings []warningJSON `json:"warnings"`
	Error    string        `json:"error"`
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(pageHTML)
}

func handleConvert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp := convertResponse{Warnings: []warningJSON{}}
	res, err := connectconverter.Convert(body)
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.YAML = string(res.YAML)
		for _, wn := range res.Warnings {
			resp.Warnings = append(resp.Warnings, warningJSON{Field: wn.Field, Message: wn.Message})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/convert", handleConvert)
	mux.HandleFunc("/", handleIndex)
	return mux
}

// Command returns the `server` subcommand that serves the converter playground.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "Serve a web playground for converting Kafka Connect configs",
		Description: `
Starts a local web server with a two-pane playground: paste a Kafka Connect
connector config on the left and see the equivalent Redpanda Connect pipeline on
the right.

  {{.BinaryName}} convert server --http localhost:4196`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "http",
				Value: "localhost:4196",
				Usage: "The address to bind the playground server to.",
			},
		},
		Action: func(c *cli.Context) error {
			addr := c.String("http")
			server := &http.Server{
				Addr:              addr,
				Handler:           newMux(),
				ReadHeaderTimeout: 5 * time.Second,
			}
			fmt.Fprintf(c.App.Writer, "Running converter playground at http://%s\n", addr)
			return server.ListenAndServe()
		},
	}
}
