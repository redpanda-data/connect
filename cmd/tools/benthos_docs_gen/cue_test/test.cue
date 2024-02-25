testCases:
	simple: #Config & {
		input: {
			label: "sample_input"
			generate: mapping: "root = 'hello'"
		}

		pipeline: processors: [
			{
				label:   "sample_transform"
				mapping: "root = this.uppercase()"
			},
			{
				switch: [
					{
						check: "count(\"total\") == 1"
						processors: [
							{
								mapping: "meta first = true"
							},
						]
					},
					{
						processors: [
							{
								mapping: "meta first = false"
							},
						]
					},
				]
			},
		]

		output: #Guarded & {
			_output: {
				label: "sample_output"
				stdout: {}
			}
		}
	}

#Guarded: self = {
	_output: #Output

	switch: cases: [
		{
			check: "errored()"
			output: reject: "failed to process message: ${! error() }"
		},
		{
			output: self._output
		},
	]
}
