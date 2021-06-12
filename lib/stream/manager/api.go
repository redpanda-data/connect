package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/Jeffail/gabs/v2"
	"github.com/gorilla/mux"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func (m *Type) registerEndpoints() {
	m.manager.RegisterEndpoint(
		"/streams",
		"GET: List all streams along with their status and uptimes."+
			" POST: Post an object of stream ids to stream configs, all"+
			" streams will be replaced by this new set.",
		m.HandleStreamsCRUD,
	)
	m.manager.RegisterEndpoint(
		"/streams/{id}",
		"Perform CRUD operations on streams, supporting POST (Create),"+
			" GET (Read), PUT (Update), PATCH (Patch update)"+
			" and DELETE (Delete).",
		m.HandleStreamCRUD,
	)
	m.manager.RegisterEndpoint(
		"/streams/{id}/stats",
		"GET a structured JSON object containing metrics for the stream.",
		m.HandleStreamStats,
	)
	m.manager.RegisterEndpoint(
		"/ready",
		"Returns 200 OK if the inputs and outputs of all running streams are connected, otherwise a 503 is returned. If there are no active streams 200 is returned.",
		m.HandleStreamReady,
	)
}

// ConfigSet is a map of stream configurations mapped by ID, which can be YAML
// parsed without losing default values inside the stream configs.
type ConfigSet map[string]stream.Config

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c ConfigSet) UnmarshalYAML(value *yaml.Node) error {
	tmpSet := map[string]yaml.Node{}
	if err := value.Decode(&tmpSet); err != nil {
		return err
	}
	for k, v := range tmpSet {
		conf := stream.NewConfig()
		if err := v.Decode(&conf); err != nil {
			return err
		}
		c[k] = conf
	}
	return nil
}

func lintStreamConfigNode(node *yaml.Node) (lints []string) {
	if node.Kind == yaml.DocumentNode && node.Content[0].Kind == yaml.MappingNode {
		node = node.Content[0]
	}
	for _, dLint := range stream.Spec().LintNode(docs.NewLintContext(), node) {
		lints = append(lints, fmt.Sprintf("line %v: %v", dLint.Line, dLint.What))
	}
	return
}

// HandleStreamsCRUD is an http.HandleFunc for returning maps of active benthos
// streams by their id, status and uptime or overwriting the entire set of
// streams.
func (m *Type) HandleStreamsCRUD(w http.ResponseWriter, r *http.Request) {
	var serverErr, requestErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if serverErr != nil {
			m.logger.Errorf("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
		}
		if requestErr != nil {
			m.logger.Debugf("Streams request CRUD Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
		}
	}()

	type confInfo struct {
		Active    bool    `json:"active"`
		Uptime    float64 `json:"uptime"`
		UptimeStr string  `json:"uptime_str"`
	}
	infos := map[string]confInfo{}

	m.lock.Lock()
	for id, strInfo := range m.streams {
		infos[id] = confInfo{
			Active:    strInfo.IsRunning(),
			Uptime:    strInfo.Uptime().Seconds(),
			UptimeStr: strInfo.Uptime().String(),
		}
	}
	m.lock.Unlock()

	switch r.Method {
	case "GET":
		var resBytes []byte
		if resBytes, serverErr = json.Marshal(infos); serverErr == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Write(resBytes)
		}
		return
	case "POST":
	default:
		requestErr = errors.New("method not supported")
		return
	}

	var setBytes []byte
	if setBytes, requestErr = ioutil.ReadAll(r.Body); requestErr != nil {
		return
	}

	if r.URL.Query().Get("chilled") != "true" {
		nodeSet := map[string]yaml.Node{}
		if requestErr = yaml.Unmarshal(setBytes, &nodeSet); requestErr != nil {
			return
		}
		var lints []string
		for k, n := range nodeSet {
			for _, l := range lintStreamConfigNode(&n) {
				keyLint := fmt.Sprintf("stream '%v': %v", k, l)
				lints = append(lints, keyLint)
				m.logger.Debugf("Streams request linting error: %v\n", keyLint)
			}
		}
		if len(lints) > 0 {
			sort.Strings(lints)
			errBytes, _ := json.Marshal(struct {
				LintErrs []string `json:"lint_errors"`
			}{
				LintErrs: lints,
			})
			w.WriteHeader(http.StatusBadRequest)
			w.Write(errBytes)
			return
		}
	}

	newSet := ConfigSet{}
	if requestErr = yaml.Unmarshal(setBytes, &newSet); requestErr != nil {
		return
	}

	toDelete := []string{}
	toUpdate := map[string]stream.Config{}
	toCreate := map[string]stream.Config{}

	for id := range infos {
		if newConf, exists := newSet[id]; !exists {
			toDelete = append(toDelete, id)
		} else {
			toUpdate[id] = newConf
		}
	}
	for id, conf := range newSet {
		if _, exists := infos[id]; !exists {
			toCreate[id] = conf
		}
	}

	deadline, hasDeadline := r.Context().Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(m.apiTimeout)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(toDelete))
	wg.Add(len(toUpdate))
	wg.Add(len(toCreate))

	errDelete := make([]error, len(toDelete))
	errUpdate := make([]error, len(toUpdate))
	errCreate := make([]error, len(toCreate))

	for i, id := range toDelete {
		go func(sid string, j int) {
			errDelete[j] = m.Delete(sid, time.Until(deadline))
			wg.Done()
		}(id, i)
	}
	i := 0
	for id, conf := range toUpdate {
		newConf := conf
		go func(sid string, sconf *stream.Config, j int) {
			errUpdate[j] = m.Update(sid, *sconf, time.Until(deadline))
			wg.Done()
		}(id, &newConf, i)
		i++
	}
	i = 0
	for id, conf := range toCreate {
		newConf := conf
		go func(sid string, sconf *stream.Config, j int) {
			errCreate[j] = m.Create(sid, *sconf)
			wg.Done()
		}(id, &newConf, i)
		i++
	}

	wg.Wait()

	errs := []string{}
	for _, err := range errDelete {
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to delete stream: %v", err))
		}
	}
	for _, err := range errUpdate {
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to update stream: %v", err))
		}
	}
	for _, err := range errCreate {
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to create stream: %v", err))
		}
	}

	if len(errs) > 0 {
		requestErr = errors.New(strings.Join(errs, "\n"))
	}
}

// HandleStreamCRUD is an http.HandleFunc for performing CRUD operations on
// individual streams.
func (m *Type) HandleStreamCRUD(w http.ResponseWriter, r *http.Request) {
	var serverErr, requestErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if serverErr != nil {
			m.logger.Errorf("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
		}
		if requestErr != nil {
			m.logger.Debugf("Streams request CRUD Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
		}
	}()

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	readConfig := func() (confOut stream.Config, lints []string, err error) {
		var confBytes []byte
		if confBytes, err = ioutil.ReadAll(r.Body); err != nil {
			return
		}

		if r.URL.Query().Get("chilled") != "true" {
			var node yaml.Node
			if requestErr = yaml.Unmarshal(confBytes, &node); requestErr != nil {
				return
			}
			lints = lintStreamConfigNode(&node)
			for _, l := range lints {
				m.logger.Infof("Stream '%v' config: %v\n", id, l)
			}
		}

		confOut = stream.NewConfig()
		err = yaml.Unmarshal(text.ReplaceEnvVariables(confBytes), &confOut)
		return
	}
	patchConfig := func(confIn stream.Config) (confOut stream.Config, err error) {
		var patchBytes []byte
		if patchBytes, err = ioutil.ReadAll(r.Body); err != nil {
			return
		}

		type aliasedIn input.Config
		type aliasedBuf buffer.Config
		type aliasedPipe pipeline.Config
		type aliasedOut output.Config

		aliasedConf := struct {
			Input    aliasedIn   `json:"input"`
			Buffer   aliasedBuf  `json:"buffer"`
			Pipeline aliasedPipe `json:"pipeline"`
			Output   aliasedOut  `json:"output"`
		}{
			Input:    aliasedIn(confIn.Input),
			Buffer:   aliasedBuf(confIn.Buffer),
			Pipeline: aliasedPipe(confIn.Pipeline),
			Output:   aliasedOut(confIn.Output),
		}
		if err = yaml.Unmarshal(patchBytes, &aliasedConf); err != nil {
			return
		}
		confOut = stream.Config{
			Input:    input.Config(aliasedConf.Input),
			Buffer:   buffer.Config(aliasedConf.Buffer),
			Pipeline: pipeline.Config(aliasedConf.Pipeline),
			Output:   output.Config(aliasedConf.Output),
		}
		return
	}

	deadline, hasDeadline := r.Context().Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(m.apiTimeout)
	}

	var conf stream.Config
	var lints []string
	switch r.Method {
	case "POST":
		if conf, lints, requestErr = readConfig(); requestErr != nil {
			return
		}
		if len(lints) > 0 {
			errBytes, _ := json.Marshal(struct {
				LintErrs []string `json:"lint_errors"`
			}{
				LintErrs: lints,
			})
			w.WriteHeader(http.StatusBadRequest)
			w.Write(errBytes)
			return
		}
		serverErr = m.Create(id, conf)
	case "GET":
		var info *StreamStatus
		if info, serverErr = m.Read(id); serverErr == nil {
			sanit, _ := info.Config().Sanitised()

			var bodyBytes []byte
			if bodyBytes, serverErr = json.Marshal(struct {
				Active    bool        `json:"active"`
				Uptime    float64     `json:"uptime"`
				UptimeStr string      `json:"uptime_str"`
				Config    interface{} `json:"config"`
			}{
				Active:    info.IsRunning(),
				Uptime:    info.Uptime().Seconds(),
				UptimeStr: info.Uptime().String(),
				Config:    sanit,
			}); serverErr != nil {
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Write(bodyBytes)
		}
	case "PUT":
		if conf, lints, requestErr = readConfig(); requestErr != nil {
			return
		}
		if len(lints) > 0 {
			errBytes, _ := json.Marshal(struct {
				LintErrs []string `json:"lint_errors"`
			}{
				LintErrs: lints,
			})
			w.WriteHeader(http.StatusBadRequest)
			w.Write(errBytes)
			return
		}
		serverErr = m.Update(id, conf, time.Until(deadline))
	case "DELETE":
		serverErr = m.Delete(id, time.Until(deadline))
	case "PATCH":
		var info *StreamStatus
		if info, serverErr = m.Read(id); serverErr == nil {
			if conf, requestErr = patchConfig(info.Config()); requestErr != nil {
				return
			}
			serverErr = m.Update(id, conf, time.Until(deadline))
		}
	default:
		requestErr = fmt.Errorf("verb not supported: %v", r.Method)
	}

	if serverErr == ErrStreamDoesNotExist {
		serverErr = nil
		http.Error(w, "Stream not found", http.StatusNotFound)
	}
	if serverErr == ErrStreamExists {
		serverErr = nil
		http.Error(w, "Stream already exists", http.StatusBadRequest)
	}
}

// HandleStreamStats is an http.HandleFunc for obtaining metrics for a stream.
func (m *Type) HandleStreamStats(w http.ResponseWriter, r *http.Request) {
	var serverErr, requestErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if serverErr != nil {
			m.logger.Errorf("Stream stats Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
		}
		if requestErr != nil {
			m.logger.Debugf("Stream request stats Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
		}
	}()

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		var info *StreamStatus
		if info, serverErr = m.Read(id); serverErr == nil {
			uptime := info.Uptime().String()
			counters := info.Metrics().GetCounters()
			timings := info.Metrics().GetTimings()

			obj := gabs.New()
			for k, v := range counters {
				obj.SetP(v, k)
			}
			for k, v := range timings {
				obj.SetP(v, k)
				obj.SetP(time.Duration(v).String(), k+"_readable")
			}
			obj.SetP(fmt.Sprintf("%v", uptime), "uptime")
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Bytes())
		}
	default:
		requestErr = fmt.Errorf("verb not supported: %v", r.Method)
	}
	if serverErr == ErrStreamDoesNotExist {
		serverErr = nil
		http.Error(w, "Stream not found", http.StatusNotFound)
	}
}

// HandleStreamReady is an http.HandleFunc for providing a ready check across
// all streams.
func (m *Type) HandleStreamReady(w http.ResponseWriter, r *http.Request) {
	var notReady []string

	m.lock.Lock()
	for k, v := range m.streams {
		if !v.IsReady() {
			notReady = append(notReady, k)
		}
	}
	m.lock.Unlock()

	if len(notReady) == 0 {
		w.Write([]byte("OK"))
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte(fmt.Sprintf("streams %v are not connected\n", strings.Join(notReady, ", "))))
}

//------------------------------------------------------------------------------
