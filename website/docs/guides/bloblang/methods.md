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

## General Purpose

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

If the result of a target function fails (due to incorrect types, failed parsing, etc) the argument is returned instead.

```coffee
doc.id = thing.id.string().catch(uuid_v4())
```

### `from(int)`

Execute a function from the context of another message in the batch. This allows you to mutate events based on the contents of other messages.

For example, `json("foo").from(1)` would extract the contents of the JSON field `foo` specifically from message index `1` of a batch.

### `from_all()`

Execute a function for all messages of the batch, and return an array of all results.

For example, `json("foo").from_all()` would extract the contents of the JSON field `foo` from all messages and return an array of the values.

### `or(function)`

If the result of the target function fails or resolves to `null`, returns the argument instead. This is an explicit method alternative to the coalesce pipe operator `|`.

```coffee
doc.id = thing.id.or(uuid_v4())
```

### `number()`

Attempt to parse a value into a number.

```coffee
result = thing.number().catch(0) + 10
```

### `string()`

Marshal a value into a string. If the value is already a string it is unchanged.

```coffee
nested_json = this.string()

# In:  {"foo":"bar"}
# Out: {"nested_json":"{\"foo\":\"bar\"}"}
```

## Object and Array Stuff

### `map_each(function)`

#### On arrays

Apply a function to each element of an array and replace the element with the result.

```coffee
new_nums = nums.map_each(
  match this {
    this < 10 => deleted()
    _ => this - 10
  }
)

# in:  {"nums":[3,11,4,17]}
# out: {"new_nums":[1,7]}
```

#### On objects

Apply a function to each value of an object and replace the value with the result. The context provided to the mapping function has a field `key` containing the value key, and a field `value`.

```coffee
new_dict = dict.map_each(value.uppercase())

# in:  {"dict":{"foo":"hello","bar":"world"}}
# out: {"new_dict":{"foo":"HELLO","bar":"WORLD"}}
```

### `sum()`

Sum the numerical values of an array. E.g. `json("foo").from_all().sum()` extracts the value of `foo` from all messages of a batch and adds them.

## String Stuff

### `format(args ...value)`

Use a value string as a format specifier in order to produce a new string using the provided arguments.

```coffee
foo = template.format(arg1, arg2)

# In:  {"template":"%s -> %s","arg1":"foo","arg2":"bar"}
# Out: {"foo":"foo -> bar"}
```

```coffee
foo = "%s(%v): %v".format(name, age, fingers)

# In:  {"name":"lance","age":37,"fingers":13}
# Out: {"foo":"lance(37): 13"}
```

### `lowercase()`

Convert a string value into lowercase.

### `uppercase()`

Convert a string value into uppercase.

[field_paths]: /docs/configuration/field_paths