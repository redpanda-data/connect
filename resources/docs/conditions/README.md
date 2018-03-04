Conditions
==========

Within the list of Benthos [processors][0] you will find the [`condition`][1]
processor, which applies a condition to every message and only propagates them
if the condition passes. Conditions themselves can modify (`not`) and combine
(`and`, `or`) other conditions, and can therefore be used to create complex
filters.

For a full list of available conditions [check out this generated document][2].

Conditions can be extremely useful for creating filters on an output. By using a
`fan_out` output broker with `condition` processors on the brokered outputs
it is possible to build curated data streams that filter on the content of each
message.

Here is an example config, where we have an output that receives only `foo`
messages, and an output that receives only `bar` messages, and a third output
that receives everything:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
      - type: file
        file:
          path: ./foo.txt
        processors:
        - type: condition
          condition:
            type: content
            content:
              operator: contains
              part: 0
              arg: foo
      - type: file
        file:
          path: ./bar.txt
        processors:
        - type: condition
          condition:
            type: content
            content:
              operator: contains
              part: 0
              arg: bar
      - type: file
        file:
          path: ./everything.txt
```

[0]: ../processors/README.md
[1]: ../processors/list.md#condition
[2]: ./list.md
