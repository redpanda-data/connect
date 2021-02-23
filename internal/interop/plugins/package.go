// Package plugins provides a way to have the new components for plugins-v2 able
// to access and "pull" the old style plugins without introducing cyclic
// dependencies. This is pretty gross.
//
// TODO: V4 Remove this
package plugins

import (
	"sync"
)

var mut sync.Mutex

// Documentation is only concerned with the name and component type of old
// plugins
var nameTypes [][2]string

// FlushNameTypes walks each registered plugin name and type and removes them.
func FlushNameTypes(fn func(nt [2]string)) {
	mut.Lock()
	defer mut.Unlock()
	for _, nt := range nameTypes {
		fn(nt)
	}
	nameTypes = nil
}

// Add an old style plugin to docs.
func Add(name, ctype string) {
	mut.Lock()
	defer mut.Unlock()
	nameTypes = append(nameTypes, [2]string{name, ctype})
}
