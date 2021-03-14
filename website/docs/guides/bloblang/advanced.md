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

However, in cases where we wish to provide multiple named parameters to a mapping we can execute them on object literals for the same effect:

However, we can still use object literals for this purpose. Imagine if we wanted a map that is the exact same as above except the pattern is `[%v]` instead, with the potential for even more patterns in the future. To do that we can pass an object with a field `value` with our target to map and a field `pattern` which allows us to specify the pattern to apply:

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

We can pull `items` out to the root with `root = items` with a [`bloblang` processor][processors.bloblang] and follow it with an [`unarchive` processor][processors.unarchive] to expand each element into its own independent message:

```yaml
pipeline:
  processors:
    - bloblang: root = this.items
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
    - bloblang: |
        let doc_root = this.without("items")
        root = this.items.map_each($doc_root.merge(this))
    - unarchive:
        format: json_array
```

[processors.bloblang]: /docs/components/processors/bloblang
[processors.unarchive]: /docs/components/processors/unarchive
