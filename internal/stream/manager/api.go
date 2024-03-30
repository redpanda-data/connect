package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/Jeffail/gabs/v2"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/stream"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

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
		"/resources/{type}/{id}",
		"POST: Create or replace a given resource configuration of a specified type. Types supported are `cache`, `input`, `output`, `processor` and `rate_limit`.",
		m.HandleResourceCRUD,
	)
	m.manager.RegisterEndpoint(
		"/streams/{id}/stats",
		"GET a structured JSON object containing metrics for the stream.",
		m.HandleStreamStats,
	)
	m.manager.RegisterEndpoint(
		"/streams/{id}",
		"Perform CRUD operations on streams, supporting POST (Create),"+
			" GET (Read), PUT (Update), PATCH (Patch update)"+
			" and DELETE (Delete).",
		m.HandleStreamCRUD,
	)
	m.manager.RegisterEndpoint(
		"/streams",
		"GET: List all streams along with their status and uptimes."+
			" POST: Post an object of stream ids to stream configs, all"+
			" streams will be replaced by this new set.",
		m.HandleStreamsCRUD,
	)
}

type lintErrors struct {
	LintErrs []string `json:"lint_errors"`
}

func (m *Type) lintCtx() docs.LintContext {
	lConf := docs.NewLintConfig(m.manager.Environment())
	lConf.BloblangEnv = bloblang.XWrapEnvironment(m.manager.BloblEnvironment()).Deactivated()
	return docs.NewLintContext(lConf)
}

func (m *Type) lintStreamConfigNode(node *yaml.Node) (lints []string) {
	for _, dLint := range stream.Spec().LintYAML(m.lintCtx(), node) {
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
			m.manager.Logger().Error("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debug("Streams request CRUD Error: %v\n", requestErr)
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

	nodeSet := map[string]yaml.Node{}
	if requestErr = yaml.Unmarshal(setBytes, &nodeSet); requestErr != nil {
		return
	}

	if r.URL.Query().Get("chilled") != "true" {
		var lints []string
		for k, n := range nodeSet {
			for _, l := range m.lintStreamConfigNode(&n) {
				keyLint := fmt.Sprintf("stream '%v': %v", k, l)
				lints = append(lints, keyLint)
				m.manager.Logger().Debug("Streams request linting error: %v\n", keyLint)
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

	toDelete := []string{}
	toUpdate := map[string]stream.Config{}
	toCreate := map[string]stream.Config{}

	spec := stream.Spec()

	for id := range infos {
		newConf, exists := nodeSet[id]
		if !exists {
			toDelete = append(toDelete, id)
		} else {
			var rawSource any
			if requestErr = newConf.Decode(&rawSource); requestErr != nil {
				return
			}
			var pConf *docs.ParsedConfig
			if pConf, requestErr = spec.ParsedConfigFromAny(&newConf); requestErr != nil {
				return
			}
			if toUpdate[id], requestErr = stream.FromParsed(m.manager.Environment(), pConf, rawSource); requestErr != nil {
				return
			}
		}
	}
	for id, conf := range nodeSet {
		if _, exists := infos[id]; !exists {
			var rawSource any
			if requestErr = conf.Decode(&rawSource); requestErr != nil {
				return
			}
			var pConf *docs.ParsedConfig
			if pConf, requestErr = spec.ParsedConfigFromAny(&conf); requestErr != nil {
				return
			}
			if toCreate[id], requestErr = stream.FromParsed(m.manager.Environment(), pConf, rawSource); requestErr != nil {
				return
			}
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
			m.manager.Logger().Error("Streams CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debug("Streams request CRUD Error: %v\n", requestErr)
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

		ignoreLints := r.URL.Query().Get("chilled") == "true"

		if confBytes, err = config.ReplaceEnvVariables(confBytes, os.LookupEnv); err != nil {
			var errEnvMissing *config.ErrMissingEnvVars
			if ignoreLints && errors.As(err, &errEnvMissing) {
				confBytes = errEnvMissing.BestAttempt
			} else {
				return
			}
		}

		var node *yaml.Node
		if node, err = docs.UnmarshalYAML(confBytes); err != nil {
			return
		}

		if !ignoreLints {
			lints = m.lintStreamConfigNode(node)
			for _, l := range lints {
				m.manager.Logger().Info("Stream '%v' config: %v\n", id, l)
			}
		}

		var rawSource any
		_ = node.Decode(&rawSource)

		var pConf *docs.ParsedConfig
		if pConf, err = stream.Spec().ParsedConfigFromAny(node); err != nil {
			return
		}
		confOut, err = stream.FromParsed(m.manager.Environment(), pConf, rawSource)
		return
	}
	patchConfig := func(confIn stream.Config) (confOut stream.Config, err error) {
		var patchBytes []byte
		if patchBytes, err = io.ReadAll(r.Body); err != nil {
			return
		}

		cRoot := value.IClone(confIn.GetRawSource())

		var pRoot any
		if err = yaml.Unmarshal(patchBytes, &pRoot); err != nil {
			return
		}

		gObj := gabs.Wrap(cRoot)
		if err = gObj.MergeFn(gabs.Wrap(pRoot), func(destination, source any) any {
			return source
		}); err != nil {
			return
		}

		var confNode yaml.Node
		if err = confNode.Encode(gObj.Data()); err != nil {
			return
		}

		var pConf *docs.ParsedConfig
		if pConf, err = stream.Spec().ParsedConfigFromAny(&confNode); err != nil {
			return
		}
		confOut, err = stream.FromParsed(m.manager.Environment(), pConf, gObj.Data())
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
			conf := info.Config()
			sanit := conf.GetRawSource()

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
	case http.MethodPut:
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
			m.manager.Logger().Error("Resource CRUD Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debug("Resource request CRUD Error: %v\n", requestErr)
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
			var cacheConf cache.Config
			if cacheConf, requestErr = cache.FromAny(m.manager.Environment(), n); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreCache(ctx, id, cacheConf)
		}
	case docs.TypeInput:
		storeFn = func(n *yaml.Node) {
			var inputConf input.Config
			if inputConf, requestErr = input.FromAny(m.manager.Environment(), n); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreInput(ctx, id, inputConf)
		}
	case docs.TypeOutput:
		storeFn = func(n *yaml.Node) {
			var outputConf output.Config
			if outputConf, requestErr = output.FromAny(m.manager.Environment(), n); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreOutput(ctx, id, outputConf)
		}
	case docs.TypeProcessor:
		storeFn = func(n *yaml.Node) {
			var procConf processor.Config
			if procConf, requestErr = processor.FromAny(m.manager.Environment(), n); requestErr != nil {
				return
			}
			serverErr = m.manager.StoreProcessor(ctx, id, procConf)
		}
	case docs.TypeRateLimit:
		storeFn = func(n *yaml.Node) {
			var rlConf ratelimit.Config
			if rlConf, requestErr = ratelimit.FromAny(m.manager.Environment(), n); requestErr != nil {
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

		ignoreLints := r.URL.Query().Get("chilled") == "true"

		if confBytes, requestErr = config.ReplaceEnvVariables(confBytes, os.LookupEnv); requestErr != nil {
			var errEnvMissing *config.ErrMissingEnvVars
			if ignoreLints && errors.As(requestErr, &errEnvMissing) {
				confBytes = errEnvMissing.BestAttempt
				requestErr = nil
			} else {
				return
			}
		}

		var node yaml.Node
		if requestErr = yaml.Unmarshal(confBytes, &node); requestErr != nil {
			return
		}
		confNode = &node

		if !ignoreLints {
			for _, l := range docs.LintYAML(m.lintCtx(), docType, &node) {
				lints = append(lints, l.Error())
				m.manager.Logger().Info("Resource '%v' config: %v\n", id, l)
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
			m.manager.Logger().Error("Stream stats Error: %v\n", serverErr)
			http.Error(w, fmt.Sprintf("Error: %v", serverErr), http.StatusBadGateway)
			return
		}
		if requestErr != nil {
			m.manager.Logger().Debug("Stream request stats Error: %v\n", requestErr)
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
