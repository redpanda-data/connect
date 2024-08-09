// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package splunk

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	siFieldURL      = "url"
	siFieldUser     = "user"
	siFieldPassword = "password"
	siFieldQuery    = "query"
	siFieldTLS      = "tls"
)

//------------------------------------------------------------------------------

func inputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.30.0").
		Categories("Services").
		Summary(`Consumes messages from Splunk.`).
		Fields(
			service.NewStringField(siFieldURL).Description("Full HTTP Search API endpoint URL.").Example("https://foobar.splunkcloud.com/services/search/v2/jobs/export"),
			service.NewStringField(siFieldUser).Description("Splunk account user."),
			service.NewStringField(siFieldPassword).Description("Splunk account password.").Secret(),
			service.NewStringField(siFieldQuery).Description("Splunk search query."),
			service.NewTLSToggledField(siFieldTLS),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	err := service.RegisterInput("splunk", inputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := inputFromParsed(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

type input struct {
	url      string
	user     string
	password string
	query    string

	client    http.Client
	body      io.ReadCloser
	reader    *bufio.Reader
	clientMut sync.Mutex
	shutSig   *shutdown.Signaller
	log       *service.Logger
}

func inputFromParsed(pConf *service.ParsedConfig, log *service.Logger) (i *input, err error) {
	i = &input{
		shutSig: shutdown.NewSignaller(),
		log:     log,
	}

	if i.url, err = pConf.FieldString(siFieldURL); err != nil {
		return
	}

	if i.user, err = pConf.FieldString(siFieldUser); err != nil {
		return
	}

	if i.password, err = pConf.FieldString(siFieldPassword); err != nil {
		return
	}

	if i.query, err = pConf.FieldString(siFieldQuery); err != nil {
		return
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(siFieldTLS); err != nil {
		return
	}

	i.client = http.Client{}
	if tlsEnabled && tlsConf != nil {
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = tlsConf
			i.client.Transport = cloned
		} else {
			i.client.Transport = &http.Transport{
				TLSClientConfig: tlsConf,
			}
		}
	}

	return
}

//------------------------------------------------------------------------------

func (i *input) Connect(ctx context.Context) error {
	i.clientMut.Lock()
	defer i.clientMut.Unlock()

	if i.reader != nil {
		return nil
	}

	payload := make(url.Values)
	payload.Set("search", "search "+i.query)
	payload.Set("output_mode", "json")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.url, strings.NewReader(payload.Encode()))
	if err != nil {
		return fmt.Errorf("failed to construct HTTP request: %s", err)
	}
	req.SetBasicAuth(i.user, i.password)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP request: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Clean up immediately if we don't have any data to read
		defer resp.Body.Close()

		if respData, err := httputil.DumpResponse(resp, true); err != nil {
			return fmt.Errorf("failed to read response: %s", err)
		} else {
			i.log.Debugf("Failed to fetch data to Splunk with status %d: %s", resp.StatusCode, string(respData))
		}

		return fmt.Errorf("HTTP request returned status: %d", resp.StatusCode)
	}

	i.body = resp.Body
	i.reader = bufio.NewReader(resp.Body)
	go func() {
		<-i.shutSig.HardStopChan()

		i.clientMut.Lock()
		if i.body != nil {
			_ = i.body.Close()
		}
		i.reader = nil
		i.clientMut.Unlock()

		i.shutSig.TriggerHasStopped()
	}()

	return nil
}

func (i *input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	i.clientMut.Lock()
	defer i.clientMut.Unlock()

	if i.reader == nil && i.body == nil {
		return nil, nil, service.ErrNotConnected
	}

	if i.body == nil {
		return nil, nil, service.ErrEndOfInput
	}

	line, err := i.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			_ = i.body.Close()
			i.body = nil
			i.reader = nil
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, fmt.Errorf("failed to read data: %s", err)
	}

	return service.NewMessage(line), func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func (i *input) Close(ctx context.Context) error {
	i.shutSig.TriggerHardStop()
	i.clientMut.Lock()
	isNil := i.reader == nil
	i.clientMut.Unlock()
	if isNil {
		return nil
	}
	select {
	case <-i.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
