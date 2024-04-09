// Package api implements a type used for creating the Benthos HTTP API.
package api

import (
	"github.com/gorilla/mux"
)

// GetMuxRoute returns a *mux.Route (the result of calling .Path or .PathPrefix
// on the provided router), where in cases where the path ends in a slash it
// will be treated as a prefix. This isn't ideal but it's as close as we can
// realistically get to the http.ServeMux behaviour with added path variables.
//
// NOTE: Eventually we can move back to http.ServeMux once
// https://github.com/golang/go/issues/61410 is available, and that'll allow us
// to make all paths explicit.
func GetMuxRoute(gMux *mux.Router, path string) *mux.Route {
	if path != "" && path[len(path)-1] == '/' {
		return gMux.PathPrefix(path)
	}
	return gMux.Path(path)
}
