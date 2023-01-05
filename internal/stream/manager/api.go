package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

//------------------------------------------------------------------------------

func (m *Type) registerEndpoints(enableCrud bool) {
	m.manager.RegisterEndpoint(
		"/ready",
		"Returns 200 OK if the inputs and outputs of all running streams are connected, otherwise a 503 is returned. If there are no active streams 200 is returned.",
		m.HandleStreamReady,
	)
	if !enableCrud {
		return
	}
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
		"/resources/{type}/{id}",
		"POST: Create or replace a given resource configuration of a specified type. Types supported are `cache`, `input`, `output`, `processor` and `rate_limit`.",
		m.HandleResourceCRUD,
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

type lintErrors struct {
	LintErrs []string `json:"lint_errors"`
}

func lintStreamConfigNode(node *yaml.Node) (lints []string) {
	for _, dLint := range stream.Spec().LintYAML(docs.NewLintContext(), node) {
		lints = append(lints, dLint.Error())
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
			m.manager.Logger().Errorf("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debugf("Streams request CRUD Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
			return
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
			_, _ = w.Write(resBytes)
		}
		return
	case "POST":
	default:
		requestErr = errors.New("method not supported")
		return
	}

	var setBytes []byte
	if setBytes, requestErr = io.ReadAll(r.Body); requestErr != nil {
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
				m.manager.Logger().Debugf("Streams request linting error: %v\n", keyLint)
			}
		}
		if len(lints) > 0 {
			errBytes, _ := json.Marshal(lintErrors{
				LintErrs: lints,
			})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(errBytes)
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

	wg := sync.WaitGroup{}
	wg.Add(len(toDelete))
	wg.Add(len(toUpdate))
	wg.Add(len(toCreate))

	errDelete := make([]error, len(toDelete))
	errUpdate := make([]error, len(toUpdate))
	errCreate := make([]error, len(toCreate))

	for i, id := range toDelete {
		go func(sid string, j int) {
			errDelete[j] = m.Delete(r.Context(), sid)
			wg.Done()
		}(id, i)
	}
	i := 0
	for id, conf := range toUpdate {
		newConf := conf
		go func(sid string, sconf *stream.Config, j int) {
			errUpdate[j] = m.Update(r.Context(), sid, *sconf)
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
			m.manager.Logger().Errorf("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debugf("Streams request CRUD Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
			return
		}
	}()

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	readConfig := func() (confOut stream.Config, lints []string, err error) {
		var confBytes []byte
		if confBytes, err = io.ReadAll(r.Body); err != nil {
			return
		}
		confBytes = config.ReplaceEnvVariables(confBytes)

		if r.URL.Query().Get("chilled") != "true" {
			var node yaml.Node
			if err = yaml.Unmarshal(confBytes, &node); err != nil {
				return
			}
			lints = lintStreamConfigNode(&node)
			for _, l := range lints {
				m.manager.Logger().Infof("Stream '%v' config: %v\n", id, l)
			}
		}

		confOut = stream.NewConfig()
		err = yaml.Unmarshal(confBytes, &confOut)
		return
	}
	patchConfig := func(confIn stream.Config) (confOut stream.Config, err error) {
		var patchBytes []byte
		if patchBytes, err = io.ReadAll(r.Body); err != nil {
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

	var conf stream.Config
	var lints []string
	switch r.Method {
	case "POST":
		if conf, lints, requestErr = readConfig(); requestErr != nil {
			return
		}
		if len(lints) > 0 {
			errBytes, _ := json.Marshal(lintErrors{
				LintErrs: lints,
			})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(errBytes)
			return
		}
		serverErr = m.Create(id, conf)
	case "GET":
		var info *StreamStatus
		if info, serverErr = m.Read(id); serverErr == nil {
			sanit, _ := info.Config().Sanitised()

			var bodyBytes []byte
			if bodyBytes, serverErr = json.Marshal(struct {
				Active    bool    `json:"active"`
				Uptime    float64 `json:"uptime"`
				UptimeStr string  `json:"uptime_str"`
				Config    any     `json:"config"`
			}{
				Active:    info.IsRunning(),
				Uptime:    info.Uptime().Seconds(),
				UptimeStr: info.Uptime().String(),
				Config:    sanit,
			}); serverErr != nil {
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(bodyBytes)
		}
	case "PUT":
		if conf, lints, requestErr = readConfig(); requestErr != nil {
			return
		}
		if len(lints) > 0 {
			errBytes, _ := json.Marshal(lintErrors{
				LintErrs: lints,
			})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(errBytes)
			return
		}
		serverErr = m.Update(r.Context(), id, conf)
	case "DELETE":
		serverErr = m.Delete(r.Context(), id)
	case "PATCH":
		var info *StreamStatus
		if info, serverErr = m.Read(id); serverErr == nil {
			if conf, requestErr = patchConfig(info.Config()); requestErr != nil {
				return
			}
			serverErr = m.Update(r.Context(), id, conf)
		}
	default:
		requestErr = fmt.Errorf("verb not supported: %v", r.Method)
	}

	if serverErr == ErrStreamDoesNotExist {
		serverErr = nil
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}
	if serverErr == ErrStreamExists {
		serverErr = nil
		http.Error(w, "Stream already exists", http.StatusBadRequest)
		return
	}
}

// HandleResourceCRUD is an http.HandleFunc for performing CRUD operations on
// resource components.
func (m *Type) HandleResourceCRUD(w http.ResponseWriter, r *http.Request) {
	var serverErr, requestErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if serverErr != nil {
			m.manager.Logger().Errorf("Resource CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debugf("Resource request CRUD Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
			return
		}
	}()

	if r.Method != "POST" {
		requestErr = fmt.Errorf("verb not supported: %v", r.Method)
		return
	}

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	var storeFn func(*yaml.Node)

	docType := docs.Type(mux.Vars(r)["type"])
	switch docType {
	case docs.TypeCache:
		storeFn = func(n *yaml.Node) {
			cacheConf := cache.NewConfig()
			if requestErr = n.Decode(&cacheConf); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreCache(ctx, id, cacheConf)
		}
	case docs.TypeInput:
		storeFn = func(n *yaml.Node) {
			inputConf := input.NewConfig()
			if requestErr = n.Decode(&inputConf); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreInput(ctx, id, inputConf)
		}
	case docs.TypeOutput:
		storeFn = func(n *yaml.Node) {
			outputConf := output.NewConfig()
			if requestErr = n.Decode(&outputConf); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreOutput(ctx, id, outputConf)
		}
	case docs.TypeProcessor:
		storeFn = func(n *yaml.Node) {
			procConf := processor.NewConfig()
			if requestErr = n.Decode(&procConf); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreProcessor(ctx, id, procConf)
		}
	case docs.TypeRateLimit:
		storeFn = func(n *yaml.Node) {
			rlConf := ratelimit.NewConfig()
			if requestErr = n.Decode(&rlConf); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreRateLimit(ctx, id, rlConf)
		}
	default:
		http.Error(w, "Var `type` must be set to one of `cache`, `input`, `output`, `processor` or `rate_limit`", http.StatusBadRequest)
		return
	}

	var confNode *yaml.Node
	var lints []string
	{
		var confBytes []byte
		if confBytes, requestErr = io.ReadAll(r.Body); requestErr != nil {
			return
		}
		confBytes = config.ReplaceEnvVariables(confBytes)

		var node yaml.Node
		if requestErr = yaml.Unmarshal(confBytes, &node); requestErr != nil {
			return
		}
		confNode = &node

		if r.URL.Query().Get("chilled") != "true" {
			for _, l := range docs.LintYAML(docs.NewLintContext(), docType, &node) {
				lints = append(lints, l.Error())
				m.manager.Logger().Infof("Resource '%v' config: %v\n", id, l)
			}
		}
	}
	if len(lints) > 0 {
		errBytes, _ := json.Marshal(lintErrors{
			LintErrs: lints,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(errBytes)
		return
	}

	storeFn(confNode)
}

// HandleStreamStats is an http.HandleFunc for obtaining metrics for a stream.
func (m *Type) HandleStreamStats(w http.ResponseWriter, r *http.Request) {
	var serverErr, requestErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if serverErr != nil {
			m.manager.Logger().Errorf("Stream stats Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debugf("Stream request stats Error: %v\n", requestErr)
			http.Error(w, fmt.Sprintf("Error: %v", requestErr), http.StatusBadRequest)
			return
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
			values := map[string]any{}
			for k, v := range info.metrics.GetCounters() {
				values[k] = v
			}
			for k, v := range info.metrics.GetTimings() {
				ps := v.Percentiles([]float64{0.5, 0.9, 0.99})
				values[k] = struct {
					P50 float64 `json:"p50"`
					P90 float64 `json:"p90"`
					P99 float64 `json:"p99"`
				}{
					P50: ps[0],
					P90: ps[1],
					P99: ps[2],
				}
			}
			values["uptime_ns"] = info.Uptime().Nanoseconds()

			jBytes, err := json.Marshal(values)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(jBytes)
		}
	default:
		requestErr = fmt.Errorf("verb not supported: %v", r.Method)
	}
	if serverErr == ErrStreamDoesNotExist {
		serverErr = nil
		http.Error(w, "Stream not found", http.StatusNotFound)
		return
	}
}

// HandleStreamReady is an http.HandleFunc for providing a ready check across
// all streams.
func (m *Type) HandleStreamReady(w http.ResponseWriter, r *http.Request) {
	var notReady []string

	m.lock.Lock()
	for k, v := range m.streams {
		if !v.IsReady() && v.IsRunning() {
			notReady = append(notReady, k)
		}
	}
	m.lock.Unlock()

	if len(notReady) == 0 {
		_, _ = w.Write([]byte("OK"))
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, "streams %v are not connected\n", strings.Join(notReady, ", "))
}
