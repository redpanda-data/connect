import "github.com/benthosdev/benthos/resources/cue/test:benthos"

testCases:
  simple: benthos.#Config & {
    input: {
      label: "sample_input"
      generate: mapping: "root = 'hello'"
    }

    pipeline: processors: [{
      label: "sample_transform"
      bloblang: "root = this.uppercase()"
    }]

    output: #Guarded & {
      _output: {
        label: "sample_output"
        stdout: {}
      }
    }
  }

  #Guarded: self = {
    _output: benthos.#Output

    switch: cases: [
      {
        check: "errored()"
        output: reject: "failed to process message: ${! error() }"
      },
      {
        output: self._output
      }
    ]
  }
