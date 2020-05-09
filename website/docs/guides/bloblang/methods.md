---
title: Bloblang Methods
sidebar_label: Methods
description: A list of Bloblang methods
---

Methods provide most of the power in Bloblang as they allow you to augment values and can be chained indefinitely:

```coffee
doc.id = thing.id.string().catch(uuid_v4())
doc.reduced_nums = thing.nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)
```

### `apply(string)`

Apply a declared map on a value.

```coffee
map thing {
  inner = first
}

foo = doc.apply("thing")

# In:  {"doc":{"first":"hello world"}}
# Out: {"foo":{"inner":"hello world"}}
```

### `catch(function)`

If the result of the target function fails (due to incorrect types, failed parsing, etc) the argument is returned instead.

```coffee
doc.id = thing.id.string().catch(uuid_v4())
```

### `for_each(function)`

Apply a function to each element of an array and replace the element with the result.

```coffee
new_nums = nums.for_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)

# in:  {"nums":[3,11,4,17]}
# out: {"new_nums":[1,7]}
```

### `from(int)`

Execute a function from the context of another message in the batch. This allows you to mutate events based on the contents of other messages.

For example, `json("foo").from(1)` would extract the contents of the JSON field `foo` specifically from message index `1` of a batch.

### `from_all()`

Execute a function for all messages of the batch, and return an array of all results.

For example, `json("foo").from_all()` would extract the contents of the JSON field `foo` from all messages and return an array of the values.

### `number()`

Attempt to parse a value into a number.

```coffee
result = thing.number().catch(0) + 10
```

### `or(function)`

If the result of the target function fails or resolves to `null`, returns the argument instead. This is an explicit method alternative to the coalesce pipe operator `|`.

```coffee
doc.id = thing.id.or(uuid_v4())
```

### `string()`

Marshal a value into a string.

```coffee
nested_json = this.string()

# In:  {"foo":"bar"}
# Out: {"nested_json":"{\"foo\":\"bar\"}"}
```

### `sum()`

Sum the numerical values of an array. E.g. `json("foo").from_all().sum()` extracts the value of `foo` from all messages of a batch and adds them.

[field_paths]: /docs/configuration/field_paths