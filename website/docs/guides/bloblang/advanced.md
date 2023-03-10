---
title: Advanced Bloblang
sidebar_label: Advanced
description: Some advanced Bloblang patterns
---

## Map Parameters

A map definition only has one input parameter, which is the context that it is called upon:

```coffee
map formatting {
  root = "(%v)".format(this)
}

root.a = this.a.apply("formatting")
root.b = this.b.apply("formatting")

# In:  {"a":"foo","b":"bar"}
# Out: {"a":"(foo)","b":"(bar)"}
```

However, we can use object literals in order to provide multiple map parameters. Imagine if we wanted a map that is the exact same as above except the pattern is `[%v]` instead, with the potential for even more patterns in the future. To do that we can pass an object with a field `value` with our target to map and a field `pattern` which allows us to specify the pattern to apply:

```coffee
map formatting {
  root = this.pattern.format(this.value)
}

root.a = {
  "value":this.a,
  "pattern":this.pattern,
}.apply("formatting")

root.b = {
  "value":this.b,
  "pattern":this.pattern,
}.apply("formatting")

# In:  {"a":"foo","b":"bar","pattern":"[%v]"}
# Out: {"a":"[foo]","b":"[bar]"}
```

## Walking the Tree

Sometimes it's necessary to perform a mapping on all values within an unknown tree structure. You can do that easily with recursive mapping:

```coffee
map unescape_values {
  root = match {
    this.type() == "object" => this.map_each(item -> item.value.apply("unescape_values")),
    this.type() == "array" => this.map_each(ele -> ele.apply("unescape_values")),
    this.type() == "string" => this.unescape_html(),
    this.type() == "bytes" => this.unescape_html(),
    _ => this,
  }
}
root = this.apply("unescape_values")

# In:  {"first":{"nested":"foo &amp; bar"},"second":10,"third":["1 &lt; 2",{"also_nested":"2 &gt; 1"}]}
# Out: {"first":{"nested":"foo & bar"},"second":10,"third":["1 < 2",{"also_nested":"2 > 1"}]}
```

## Message Expansion

Expanding a single message into multiple messages can be done by mapping messages into an array and following it up with an [`unarchive` processor][processors.unarchive]. For example, given documents of this format:

```json
{
  "id": "foobar",
  "items": [
    {"content":"foo"},
    {"content":"bar"},
    {"content":"baz"}
  ]
}
```

We can pull `items` out to the root with `root = items` with a [`mapping` processor][processors.mapping] and follow it with an [`unarchive` processor][processors.unarchive] to expand each element into its own independent message:

```yaml
pipeline:
  processors:
    - mapping: root = this.items
    - unarchive:
        format: json_array
```

However, most of the time we also need to map the elements before expanding them, and often that includes copying fields outside of our target array. We can do that with methods such as `map_each` and `merge`:

```coffee
root = this.items.map_each(ele -> this.without("items").merge(ele))

# In:  {"id":"foobar","items":[{"content":"foo"},{"content":"bar"},{"content":"baz"}]}
# Out: [{"content":"foo","id":"foobar"},{"content":"bar","id":"foobar"},{"content":"baz","id":"foobar"}]
```

However, the above mapping is slightly inefficient as we would create a copy of our source object for each element with the `this.without("items")` part. A more efficient way to do this would be to capture that query within a variable:

```coffee
let doc_root = this.without("items")
root = this.items.map_each($doc_root.merge(this))

# In:  {"id":"foobar","items":[{"content":"foo"},{"content":"bar"},{"content":"baz"}]}
# Out: [{"content":"foo","id":"foobar"},{"content":"bar","id":"foobar"},{"content":"baz","id":"foobar"}]
```

Also note that when we set `doc_root` we remove the field `items` from the target document. The full config would now be:

```yaml
pipeline:
  processors:
    - mapping: |
        let doc_root = this.without("items")
        root = this.items.map_each($doc_root.merge(this))
    - unarchive:
        format: json_array
```

## Creating CSV

Benthos has a few different ways of outputting a stream of CSV data. However, the best way to do it is by converting the documents into CSV rows with Bloblang as this gives you full control over exactly how the schema is generated, erroneous data is handled, and escaping of column data is performed.

A common and simple use case is to simply flatten documents and write out the column values in alphabetical order. The first row we generate should also be prefixed with a row containing those column names. Here's a mapping that achieves this by using a `count` function to detect the very first invocation of the mapping in a stream pipeline:

```coffee
map escape_csv {
  root = if this.re_match("[\"\n,]+") {
    "\"" + this.replace_all("\"", "\"\"") + "\""
  } else {
    this
  }
}

# Extract key/value pairs as an array and sort by the key
let kvs = this.key_values().sort_by(v -> v.key)

# Create a header prefix for our output only on the first row
let header = if count("rows_in_file") == 1 {
  $kvs.map_each(kv -> kv.key.apply("escape_csv")).join(",") + "\n"
} else { "" }

root = $header + $kvs.map_each(kv -> kv.value.string().apply("escape_csv")).join(",")
```

And with this mapping we can write the data to a newly created CSV file using an output with a simple `lines` codec:

```yaml
output:
  file:
    path: ./result.csv
    codec: lines
```

Perhaps the first expansion of this mapping that would be worthwhile is to add an explicit list of column names, or at least confirm that the number of values in a row matches an expected count.

[processors.mapping]: /docs/components/processors/mapping
[processors.unarchive]: /docs/components/processors/unarchive
