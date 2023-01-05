Benthos WASM Plugins
====================

In this directory are libraries and examples tailored to help developers create WASM plugins that can be run by the Benthos [`wasm` processor][processor.wasm]. It's possible to write WASM plugins in any language that compiles to a WASM module. However, given the complexity in passing allocated memory between the module and the host process it's much easier to use the libraries provided here as the basis for your plugin.

Most of these are adapted from the fantastic range of examples provided by [the Wazero library][wazero_examples]. Our goal is to eventually provide libraries and examples for all popular languages and we'll be tackling them one at a time based on demand. Please be patient but also make [yourself heard][community].

[processor.wasm]: https://www.benthos.dev/docs/components/processors/wasm
[wazero_examples]: https://github.com/tetratelabs/wazero/tree/main/examples
[community]: https://www.benthos.dev/community
