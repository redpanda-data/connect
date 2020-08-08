---
title: Advanced Bloblang
sidebar_label: Advanced
description: Some advanced Bloblang patterns
---

## Map Parameters

A map definition isn't able to formally specify input parameters:

```coffee
map formating {
  root = this
  foo = "(%v)".format(foo)
  bar = "(%v)".format(bar)
}

first = doc.apply("formating")

# In:  {"doc":{"foo":"f","bar":"b","baz":"bz"}}
# Out: {"first":{"bar":"(b)","baz":"bz","foo":"(f)"}}
```

However, we can still use object literals for this purpose. Imagine if we wanted a map that is the exact same as above except the pattern is `[%v]` instead, with the potential for even more patterns in the future. To do that we can pass an object with a field `value` with our target to map and a field `pattern` which allows us to specify the pattern to apply:

```coffee
map formatting {
  root = value
  foo = pattern.format(value.foo)
  bar = pattern.format(value.bar)
}

first = {"value":doc,"pattern":"(%v)"}.apply("formatting")
second = {"value":doc,"pattern":"[%v]"}.apply("formatting")

# In:  {"doc":{"foo":"f","bar":"b","baz":"bz"}}
# Out: {"first":{"bar":"(b)","baz":"bz","foo":"(f)"},"second":{"bar":"[b]","baz":"bz","foo":"[f]"}}
```

## Walking the Tree

Sometimes it's necessary to perform a mapping on all values within an unknown tree structure. You can do that easily with recursive mapping:

```coffee
map unescape_values {
  root = match {
    this.type() == "object" => this.map_each(this.value.apply("unescape_values")),
    this.type() == "array" => this.map_each(this.apply("unescape_values")),
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

Expanding a message into >1 messages can be done by using Bloblang to map messages into an array and following it up with an [`unarchive` processor][processors.unarchive]. For example, given documents of this format:

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

In the simple case we can simply pull `items` out to the root with `root = items` in a [`bloblang` processor][processors.bloblang] and follow it with an [`unarchive` processor][processors.unarchive] to expand each element into its own independent message:

```yaml
pipeline:
  processors:
    - bloblang: root = items
    - unarchive:
        format: json_array
```

However, most of the time we also need to map the elements before expanding them, and often that includes copying fields outside of our target array. We can do that with context variables:

```coffee
let doc_root = this.without("items")
root = items.map_each($doc_root.merge(this))

# In:  {"id":"foobar","items":[{"content":"foo"},{"content":"bar"},{"content":"baz"}]}
# Out: [{"content":"foo","id":"foobar"},{"content":"bar","id":"foobar"},{"content":"baz","id":"foobar"}]
```

Also note that when we set `doc_root` we remove the field `items` from the target document. The full config would now be:

```yaml
pipeline:
  processors:
    - bloblang: |
        let doc_root = this.without("items")
        root = items.map_each($doc_root.merge(this))
    - unarchive:
        format: json_array
```

[processors.bloblang]: /docs/components/processors/bloblang
[processors.unarchive]: /docs/components/processors/unarchive
