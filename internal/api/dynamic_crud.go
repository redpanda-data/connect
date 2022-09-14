package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"
)

// dynamicConfMgr maintains a map of config hashes to ids for dynamic
// inputs/outputs and thereby tracks whether a new configuration has changed for
// a particular id.
type dynamicConfMgr struct {
	configHashes map[string]string
}

func newDynamicConfMgr() *dynamicConfMgr {
	return &dynamicConfMgr{
		configHashes: map[string]string{},
	}
}

// Set will cache the config hash as the latest for the id and returns whether
// this hash is different to the previous config.
func (d *dynamicConfMgr) Set(id string, conf []byte) bool {
	hasher := sha256.New()
	hasher.Write(conf)
	newHash := hex.EncodeToString(hasher.Sum(nil))

	if hash, exists := d.configHashes[id]; exists {
		if hash == newHash {
			// Same config as before, ignore.
			return false
		}
	}

	d.configHashes[id] = newHash
	return true
}

// Matches checks whether a provided config matches an existing config for the
// same id.
func (d *dynamicConfMgr) Matches(id string, conf []byte) bool {
	if hash, exists := d.configHashes[id]; exists {
		hasher := sha256.New()
		hasher.Write(conf)
		newHash := hex.EncodeToString(hasher.Sum(nil))

		if hash == newHash {
			// Same config as before.
			return true
		}
	}

	return false
}

// Remove will delete a cached hash for id if there is one.
func (d *dynamicConfMgr) Remove(id string) {
	delete(d.configHashes, id)
}

//------------------------------------------------------------------------------

// Dynamic is a type for exposing CRUD operations on dynamic broker
// configurations as an HTTP interface. Events can be registered for listening
// to configuration changes, and these events should be forwarded to the
// dynamic broker.
type Dynamic struct {
	onUpdate func(ctx context.Context, id string, conf []byte) error
	onDelete func(ctx context.Context, id string) error

	// configs is a map of the latest sanitised configs from our CRUD clients.
	configs      map[string][]byte
	configHashes *dynamicConfMgr
	configsMut   sync.Mutex

	// ids is a map of dynamic components that are currently active and their
	// start times.
	ids    map[string]time.Time
	idsMut sync.Mutex
}

// NewDynamic creates a new Dynamic API type.
func NewDynamic() *Dynamic {
	return &Dynamic{
		onUpdate:     func(ctx context.Context, id string, conf []byte) error { return nil },
		onDelete:     func(ctx context.Context, id string) error { return nil },
		configs:      map[string][]byte{},
		configHashes: newDynamicConfMgr(),
		ids:          map[string]time.Time{},
	}
}

//------------------------------------------------------------------------------

// OnUpdate registers a func to handle CRUD events where a request wants to set
// a new value for a dynamic configuration. An error should be returned if the
// configuration is invalid or the component failed.
func (d *Dynamic) OnUpdate(onUpdate func(ctx context.Context, id string, conf []byte) error) {
	d.onUpdate = onUpdate
}

// OnDelete registers a func to handle CRUD events where a request wants to
// remove a dynamic configuration. An error should be returned if the component
// failed to close.
func (d *Dynamic) OnDelete(onDelete func(ctx context.Context, id string) error) {
	d.onDelete = onDelete
}

// Stopped should be called whenever an active dynamic component has closed,
// whether by naturally winding down or from a request.
func (d *Dynamic) Stopped(id string) {
	d.idsMut.Lock()
	defer d.idsMut.Unlock()

	delete(d.ids, id)
}

// Started should be called whenever an active dynamic component has started
// with a new configuration. A normalised form of the configuration should be
// provided and will be delivered to clients that query the component contents.
func (d *Dynamic) Started(id string, config []byte) {
	d.idsMut.Lock()
	d.ids[id] = time.Now()
	d.idsMut.Unlock()

	if len(config) > 0 {
		d.configsMut.Lock()
		d.configs[id] = config
		d.configsMut.Unlock()
	}
}

//------------------------------------------------------------------------------

// HandleList is an http.HandleFunc for returning maps of active dynamic
// components by their id to uptime.
func (d *Dynamic) HandleList(w http.ResponseWriter, r *http.Request) {
	var httpErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if httpErr != nil {
			http.Error(w, "Internal server error", http.StatusBadGateway)
			return
		}
	}()

	type confInfo struct {
		Uptime    string `json:"uptime"`
		Config    any    `json:"config"`
		ConfigRaw string `json:"config_raw"`
	}
	uptimes := map[string]confInfo{}

	d.idsMut.Lock()
	for k, v := range d.ids {
		uptimes[k] = confInfo{
			Uptime:    time.Since(v).String(),
			Config:    nil,
			ConfigRaw: "",
		}
	}
	d.idsMut.Unlock()

	d.configsMut.Lock()
	for k, v := range d.configs {
		var confStructured any
		if httpErr = yaml.Unmarshal(v, &confStructured); httpErr != nil {
			return
		}
		info := confInfo{
			Uptime:    "stopped",
			Config:    confStructured,
			ConfigRaw: string(v),
		}
		if existingInfo, exists := uptimes[k]; exists {
			info.Uptime = existingInfo.Uptime
		}
		uptimes[k] = info
	}
	d.configsMut.Unlock()

	var resBytes []byte
	if resBytes, httpErr = json.Marshal(uptimes); httpErr == nil {
		_, _ = w.Write(resBytes)
	}
}

func (d *Dynamic) handleGETInput(w http.ResponseWriter, r *http.Request) error {
	id := mux.Vars(r)["id"]

	d.configsMut.Lock()
	conf, exists := d.configs[id]
	d.configsMut.Unlock()
	if !exists {
		http.Error(w, fmt.Sprintf("Dynamic component '%v' is not active", id), http.StatusNotFound)
		return nil
	}
	_, _ = w.Write(conf)
	return nil
}

func (d *Dynamic) handlePOSTInput(w http.ResponseWriter, r *http.Request) error {
	id := mux.Vars(r)["id"]

	reqBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	d.configsMut.Lock()
	matched := d.configHashes.Matches(id, reqBytes)
	d.configsMut.Unlock()
	if matched {
		return nil
	}

	if err := d.onUpdate(r.Context(), id, reqBytes); err != nil {
		return err
	}

	d.configsMut.Lock()
	d.configHashes.Set(id, reqBytes)
	d.configsMut.Unlock()
	return nil
}

func (d *Dynamic) handleDELInput(w http.ResponseWriter, r *http.Request) error {
	id := mux.Vars(r)["id"]

	if err := d.onDelete(r.Context(), id); err != nil {
		return err
	}

	d.configsMut.Lock()
	d.configHashes.Remove(id)
	delete(d.configs, id)
	d.configsMut.Unlock()

	return nil
}

// HandleCRUD is an http.HandleFunc for performing CRUD operations on dynamic
// components by their ids.
func (d *Dynamic) HandleCRUD(w http.ResponseWriter, r *http.Request) {
	var httpErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if httpErr != nil {
			http.Error(w, fmt.Sprintf("Error: %v", httpErr), http.StatusBadGateway)
			return
		}
	}()

	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "POST":
		httpErr = d.handlePOSTInput(w, r)
	case "GET":
		httpErr = d.handleGETInput(w, r)
	case "DELETE":
		httpErr = d.handleDELInput(w, r)
	default:
		httpErr = fmt.Errorf("verb not supported: %v", r.Method)
	}
}
