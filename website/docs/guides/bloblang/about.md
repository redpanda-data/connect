---
title: Bloblang
sidebar_label: About
description: The Benthos native mapping language
---

Bloblang, or blobl for short (pronounced "blobble"), is a language designed specifically for mapping unstructured data. It's a safe, fast, and powerful way to perform document mapping within Benthos.

Bloblang is available as a [mapping processor][blobl.proc], as a [condition][blobl.cond], and it's also possible to use blobl queries in [function interpolations][blobl.interp].

You can also execute bloblang mappings on the command line with the `blobl` subcommand:

```shell
$ cat data.jsonl | benthos blobl 'foo.(bar | baz).buz'
```

## Assignment

An assignment consists of a left-hand-side assignment target and a right-hand-side mapping query.

Map a new document by using [dot paths][field_paths]:

```coffee
new_doc.id = thing.id
new_doc.type = "yo"

# Keyword `root` refers to the root of the mapped document
# Keyword `this` refers to the root of the query context
# Both are optional
root.new_doc.content = this.thing.doc.message

# In:  {"thing":{"id":"wat1","doc":{"title":"wut","message":"hello world"}}}
# Out: {"new_doc":{"content":"hello world","id":"wat1","type":"yo"}}
```

The newly mapped document starts off empty, if you wish to begin your mapping with a full copy of the input object then start your map by assigning it to the root:

```coffee
root = this
foo = "added value"

# In:  {"id":"wat1","message":"hello world"}
# Out: {"id":"wat1","message":"hello world","foo":"added value"}
```

Variables help with value reuse:

```coffee
# Set a temporary variable
let foo = "yo"

new_doc.type = $foo # Reference a variable with $
```

You can also use Bloblang to set and reference metadata values:

```coffee
# Delete all existing metadata
meta = deleted()

# Set a metadata value
meta bar = "hello world"

# Reference a metadata value from the input message
new_doc.bar = meta("kafka_topic")
```

## Coalesce

The pipe operator (`|`) used within brackets allows you to coalesce values within a path:

```coffee
new_doc.type = thing.(article | comment | this).type

# In:  {"thing":{"article":{"type":"foo"}}}
# Out: {"new_doc":{"type":"foo"}}

# In:  {"thing":{"comment":{"type":"bar"}}}
# Out: {"new_doc":{"type":"bar"}}

# In:  {"thing":{"type":"baz"}}
# Out: {"new_doc":{"type":"baz"}}
```

## Literals and Arithmetic

Bloblang supports number, boolean, string, null, array and object literals:

```coffee
root = [
  7, false, "string", null, {
    "first": 11,
    "second": {"foo":"bar"},
    "third": """multiple
lines on this
string"""
  }
]

# In:  {}
# Out: [7,false,"string",null,{"first":11,"second":{"foo":"bar"},"third":"multiple\nlines on this\nstring"}]
```

## Comments

You might've already spotted, comments are started with a hash (`#`) and end with a line break:

```coffee
root = some.value # And now this is a comment
```

## Boolean Logic and Arithmetic

Bloblang supports a range of boolean operators `!`, `>`, `>=`, `==`, `<`, `<=`, `&&`, `||` and arithmetic operators `+`, `-`, `*`, `/`, `%`:

```coffee
is_big = number > 100
multiplied = number * 7

# In:  {"number":50}
# Out: {"is_big":false,"multiplied":350}

# In:  {"number":150}
# Out: {"is_big":true,"multiplied":1050}
```

## Pattern Matching

A `match` expression allows you to perform conditional mappings on a value using boolean logic:

```coffee
new_doc = match doc {
  type == "article" => article
  type == "comment" => comment
  _ => this
}

# In:  {"doc":{"type":"article","article":{"id":"foo","content":"qux"}}}
# Out: {"new_doc":{"id":"foo","content":"qux"}}

# In:  {"doc":{"type":"comment","comment":{"id":"bar","content":"quz"}}}
# Out: {"new_doc":{"id":"bar","content":"quz"}}

# In:  {"doc":{"type":"neither","content":"some other stuff unchanged"}}
# Out: {"new_doc":{"type":"neither","content":"some other stuff unchanged"}}
```

Match cases can specify a literal value for simple comparison:

```coffee
root = this
type = match type { "doc" => "document", "art" => "article", _ => this }

# In:  {"type":"doc","foo":"bar"}
# Out: {"type":"document","foo":"bar"}
```

The match context can also be left unset, and the catch-all case can also be omitted:

```coffee
new_doc = match {
  doc.type == "article" => doc.article
  doc.type == "comment" => doc.comment
}

# In:  {"doc":{"type":"neither","content":"some other stuff unchanged"}}
# Out: {"doc":{"type":"neither","content":"some other stuff unchanged"}}
```

If no case matches then the mapping is skipped entirely, hence we would end up with the original document in this case.

## Functions

Functions can be placed anywhere and allow you to extract information from your environment, generate values, or access data from the underlying message being mapped:

```coffee
doc.id = uuid_v4()
doc.received_at = timestamp_unix()
doc.host = hostname()
```

You can find a full list of functions in [this doc][blobl.functions].

## Methods

Methods provide most of the power in Bloblang as they allow you to augment values by chaining them:

```coffee
doc.id = thing.id.string().catch(uuid_v4())
doc.reduced_nums = thing.nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)
```

You can find a full list of methods in [this doc][blobl.methods].

## Maps

It's possible to declare reusable maps for common operations:

```coffee
map things {
  first  = thing_one
  second = thing_two
}

foo = value_one.apply("things")
bar = value_two.apply("things")

# In:  {"value_one":{"thing_one":"hey","thing_two":"yo"},"value_two":{"thing_one":"sup","thing_two":"waddup"}}
# Out: {"foo":{"first":"hey","second":"yo"},"bar":{"first":"sup","second":"waddup"}}
```

And even import maps from other files:

```coffee
import "./common_maps.blobl"

foo = value_one.apply("things")
bar = value_two.apply("things")
```

## Filtering

By assigning the root of a mapped document to the `deleted()` function you can delete a message entirely:

```coffee
# Filter all messages that have fewer than 10 URLs.
root = match {
  doc.urls.length() < 10 => deleted()
}
```

## Error Handling

Functions and methods can fail under certain circumstances, such as when they receive types they aren't able to act upon. These failures, when not caught, will cause the entire mapping to fail. However, the [method `catch`][methods.catch] can be used in order to return a value when a failure occurs instead:

```coffee
# Map an empty array to `foo` if the field `bar` is not a string.
foo = bar.split(",").catch([])
```

Since `catch` is a method it can also be attached to bracketed map expressions:

```coffee
# Map `false` if any of the operations in this boolean query fail.
thing = ( foo > bar && baz.contains("wut") ).catch(false)
```

The `catch` method only acts on errors, sometimes it's also useful to set a fall back value when a query returns `null` in which case the [method `or`][methods.or] can be used the same way:

```coffee
# Map "default" if either the element index 5 does not exist, or the underlying
# element is `null`.
foo = bar.index(5).or("default")
```

[field_paths]: /docs/configuration/field_paths
[blobl.proc]: /docs/components/processors/bloblang
[blobl.cond]: /docs/components/conditions/bloblang
[blobl.interp]: /docs/configuration/interpolation#bloblang-queries
[blobl.functions]: /docs/guides/bloblang/functions
[blobl.methods]: /docs/guides/bloblang/methods
[methods.catch]: /docs/guides/bloblang/methods#catch
[methods.or]: /docs/guides/bloblang/methods#or