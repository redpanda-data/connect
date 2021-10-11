// Package bloblang provides high level APIs for registering custom Bloblang
// plugins, as well as for parsing and executing Bloblang mappings.
//
// For a video guide on Benthos plugins check out: https://youtu.be/uH6mKw-Ly0g
// And an example repo containing component plugins and tests can be found at:
// https://github.com/benthosdev/benthos-plugin-example
//
// Plugins can either be registered globally, and will be accessible to any
// component parsing Bloblang expressions in the executable, or they can be
// registered as part of an isolated environment.
package bloblang
