---
title: Bloblang
sidebar_label: About
description: The Benthos native mapping language
---

Bloblang, or blobl for short, is a language designed for mapping data of a wide variety of forms. It's a safe, fast, and powerful way to perform document mapping within Benthos.

Bloblang is available as a [processor][blobl.proc] and it's also possible to use blobl queries in [function interpolations][blobl.interp].

You can also execute Bloblang mappings on the command-line with the `blobl` subcommand:

```shell
$ cat data.jsonl | benthos blobl 'foo.(bar | baz).buz'
```

## Assignment

A Bloblang mapping expresses how to create a new document by extracting data from an existing input document. Assignments consist of a [dot path][field_paths] argument on the left-hand side describing a field to be created within the new document, and a right-hand side query describing what the content of the new field should be.

The keyword `root` on the left-hand side refers to the root of the new document, the keyword `this` on the right-hand side refers to the current context of the query, which is the read-only input document when querying from the root of a mapping:

```coffee
root.id = this.thing.id
root.type = "yo"

# Both `root` and `this` are optional, and will be inferred in their absence.
content = thing.doc.message

# In:  {"thing":{"id":"wat1","doc":{"title":"wut","message":"hello world"}}}
# Out: {"content":"hello world","id":"wat1","type":"yo"}
```

Since the document being created starts off empty it is sometimes useful to begin a mapping by copying the entire contents of the input document, which can be expressed by assigning `this` to `root`.

```coffee
root = this
root.foo = "added value"

# In:  {"id":"wat1","message":"hello world"}
# Out: {"id":"wat1","message":"hello world","foo":"added value"}
```

### Non-structured Results

Your newly mapped document doesn't need to be a structured object, simply assign a value type to the `root` of your document:

```coffee
root = this.foo

# In:  {"foo":"hello world"}
# Out: hello world
```

And the resulting message payload will be the raw value you've assigned.

### Deleting

It's possible to selectively delete fields from an object by assigning the function `deleted()` to the field path:

```coffee
root = this
root.bar = deleted()

# In:  {"id":"wat1","message":"hello world","bar":"remove me"}
# Out: {"id":"wat1","message":"hello world"}
```

### Variables

Another type of assignment is a `let` statement, which creates a variable that can be referenced elsewhere within a mapping. Variables are discarded at the end of the mapping and are mostly useful for query reuse. Variables are referenced within queries with `$`:

```coffee
# Set a temporary variable
let foo = "yo"

root.new_doc.type = $foo
```

### Metadata

Benthos messages contain metadata that is separate from the main payload, in Bloblang you can query and modify the metadata of messages with the `meta` assignment keyword and query function:

```coffee
# Delete all existing metadata
meta = deleted()

# Set a metadata value
meta bar = "hello world"

# Reference a metadata value from the input message
root.new_doc.bar = meta("kafka_topic")
```

### Special Characters in Paths

Quotes can be used to describe sections of a field path that contain whitespace, dots or other special characters:

```coffee
# Use quotes around a path segment in order to include whitespace or dots within
# the path
root."foo.bar".baz = this."buz bev".fub

# In:  {"buz bev":{"fub":"hello world"}}
# Out: {"foo.bar":{"baz":"hello world"}}
```

## Coalesce

The pipe operator (`|`) used within brackets allows you to coalesce values within a path:

```coffee
root.new_doc.type = this.thing.(article | comment | this).type

# In:  {"thing":{"article":{"type":"foo"}}}
# Out: {"new_doc":{"type":"foo"}}

# In:  {"thing":{"comment":{"type":"bar"}}}
# Out: {"new_doc":{"type":"bar"}}

# In:  {"thing":{"type":"baz"}}
# Out: {"new_doc":{"type":"baz"}}
```

This is a called a bracketed mapping and within it the context changes to the path value it is added to, therefore in the above example `this` within the brackets refers to the contents of `this.thing`.

## Literals

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
root = this.some.value # And now this is a comment
```

## Boolean Logic and Arithmetic

Bloblang supports a range of boolean operators `!`, `>`, `>=`, `==`, `<`, `<=`, `&&`, `||` and arithmetic operators `+`, `-`, `*`, `/`, `%`:

```coffee
root.is_big = this.number > 100
root.multiplied = this.number * 7

# In:  {"number":50}
# Out: {"is_big":false,"multiplied":350}

# In:  {"number":150}
# Out: {"is_big":true,"multiplied":1050}
```

## Conditional Mapping

Use `if` expressions to perform maps conditionally:

```coffee
root = this
root.sorted_foo = if this.foo.type() == "array" { this.foo.sort() }

# In:  {"foo":"foobar"}
# Out: {"foo":"foobar"}

# In:  {"foo":["foo","bar"]}
# Out: {"foo":["foo","bar"],"sorted_foo":["bar","foo"]}
```

And add an `else` for alternative maps:

```coffee
root.foo = if this.exists("foo") {
  this.foo
} else {
  "default"
}

# In:  {"foo":"foobar"}
# Out: {"foo":"foobar"}

# In:  {}
# Out: {"foo":"default"}
```

## Pattern Matching

A `match` expression allows you to perform conditional mappings on a value using boolean logic:

```coffee
root.new_doc = match this.doc {
  this.type == "article" => this.article
  this.type == "comment" => this.comment
  _ => this
}

# In:  {"doc":{"type":"article","article":{"id":"foo","content":"qux"}}}
# Out: {"new_doc":{"id":"foo","content":"qux"}}

# In:  {"doc":{"type":"comment","comment":{"id":"bar","content":"quz"}}}
# Out: {"new_doc":{"id":"bar","content":"quz"}}

# In:  {"doc":{"type":"neither","content":"some other stuff unchanged"}}
# Out: {"new_doc":{"type":"neither","content":"some other stuff unchanged"}}
```

Within a match block the context of `this` changes to the pattern matched expression, therefore `this` within the match expression above refers to `this.doc`.

Match cases can specify a literal value for simple comparison:

```coffee
root = this
root.type = match this.type { "doc" => "document", "art" => "article", _ => this }

# In:  {"type":"doc","foo":"bar"}
# Out: {"type":"document","foo":"bar"}
```

The match expression can also be left unset which means the context remains unchanged, and the catch-all case can also be omitted:

```coffee
root.new_doc = match {
  this.doc.type == "article" => this.doc.article
  this.doc.type == "comment" => this.doc.comment
}

# In:  {"doc":{"type":"neither","content":"some other stuff unchanged"}}
# Out: {"doc":{"type":"neither","content":"some other stuff unchanged"}}
```

If no case matches then the mapping is skipped entirely, hence we would end up with the original document in this case.

## Functions

Functions can be placed anywhere and allow you to extract information from your environment, generate values, or access data from the underlying message being mapped:

```coffee
root.doc.id = uuid_v4()
root.doc.received_at = now()
root.doc.host = hostname()
```

You can find a full list of functions in [this doc][blobl.functions].

## Methods

Methods provide most of the power in Bloblang as they allow you to augment query values and can be added to any expression:

```coffee
root.doc.id = this.thing.id.string().catch(uuid_v4())
root.doc.reduced_nums = this.thing.nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)
root.has_good_taste = ["pikachu","mewtwo","magmar"].contains(this.user.fav_pokemon)
```

You can find a full list of methods in [this doc][blobl.methods].

## Maps

It's possible to declare reusable maps for common operations:

```coffee
map things {
  root.first  = this.thing_one
  root.second = this.thing_two
}

root.foo = this.value_one.apply("things")
root.bar = this.value_two.apply("things")

# In:  {"value_one":{"thing_one":"hey","thing_two":"yo"},"value_two":{"thing_one":"sup","thing_two":"waddup"}}
# Out: {"foo":{"first":"hey","second":"yo"},"bar":{"first":"sup","second":"waddup"}}
```

Within a map the keyword `root` refers to a newly created document, and `this` refers to whatever the map is applied to.

## Import Maps

It's possible to import maps defined in a file with an `import` statement:

```coffee
import "./common_maps.blobl"

root.foo = this.value_one.apply("things")
root.bar = this.value_two.apply("things")
```

Imports from a Bloblang mapping within a Benthos config are relative to the process running the config. Imports from an imported file are relative to the file that is importing it.

## Filtering

By assigning the root of a mapped document to the `deleted()` function you can delete a message entirely:

```coffee
# Filter all messages that have fewer than 10 URLs.
root = match {
  this.doc.urls.length() < 10 => deleted()
}
```

## Error Handling

Functions and methods can fail under certain circumstances, such as when they receive types they aren't able to act upon. These failures, when not caught, will cause the entire mapping to fail. However, the [method `catch`][methods.catch] can be used in order to return a value when a failure occurs instead:

```coffee
# Map an empty array to `foo` if the field `bar` is not a string.
root.foo = this.bar.split(",").catch([])
```

Since `catch` is a method it can also be attached to bracketed map expressions:

```coffee
# Map `false` if any of the operations in this boolean query fail.
root.thing = ( this.foo > this.bar && this.baz.contains("wut") ).catch(false)
```

And one of the more powerful features of Bloblang is that a single `catch` method at the end of a chain of methods can recover errors from any method in the chain:

```coffee
# Catch errors caused by:
# - foo not existing
# - foo not being a string
# - an element from split foo not being a valid JSON string
root.things = this.foo.split(",").map_each( this.parse_json() ).catch([])

# Specifically catch a JSON parse error
root.things = this.foo.split(",").map_each( this.parse_json().catch({}) )
```

However, the `catch` method only acts on errors, sometimes it's also useful to set a fall back value when a query returns `null` in which case the [method `or`][methods.or] can be used the same way:

```coffee
# Map "default" if either the element index 5 does not exist, or the underlying
# element is `null`.
root.foo = this.bar.index(5).or("default")
```

[field_paths]: /docs/configuration/field_paths
[blobl.proc]: /docs/components/processors/bloblang
[blobl.interp]: /docs/configuration/interpolation#bloblang-queries
[blobl.functions]: /docs/guides/bloblang/functions
[blobl.methods]: /docs/guides/bloblang/methods
[methods.catch]: /docs/guides/bloblang/methods#catch
[methods.or]: /docs/guides/bloblang/methods#or