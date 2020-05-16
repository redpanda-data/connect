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

### `apply`

Apply a declared map on a value.

```coffee
map thing {
  inner = first
}

foo = doc.apply("thing")

# In:  {"doc":{"first":"hello world"}}
# Out: {"foo":{"inner":"hello world"}}
```

### `catch`

If the result of a target function fails (due to incorrect types, failed parsing, etc) the argument is returned instead.

```coffee
doc.id = thing.id.string().catch(uuid_v4())
```

### `from`

Execute a function from the context of another message in the batch. This allows you to mutate events based on the contents of other messages. For example, the map:

```coffee
root = this
foo = json("foo").from(1)
```

Would extract the contents of the JSON field `foo` specifically from message index `1` of a batch, effectively overriding the field `foo` for all messages of a batch to that of message 1.

### `from_all`

Execute a function for all messages of the batch, and return an array of all results.

```coffee
root = this
foo_summed = json("foo").from_all().sum()
```

### `or`

If the result of the target function fails or resolves to `null`, returns the argument instead. This is an explicit method alternative to the coalesce pipe operator `|`.

```coffee
doc.id = thing.id.or(uuid_v4())
```

### `number`

Attempt to parse a value into a number.

```coffee
result = thing.number().catch(0) + 10
```

### `string`

Marshal a value into a string. If the value is already a string it is unchanged.

```coffee
nested_json = this.string()

# In:  {"foo":"bar"}
# Out: {"nested_json":"{\"foo\":\"bar\"}"}
```

## Object and Array Stuff

### `append`

Returns an array with new elements appended to the end.

```coffee
foo = foo.append("and", "this")

# In:  {"foo":["bar","baz"]}
# Out: {"foo":["bar","baz","and","this"]}
```

### `contains`

Checks whether a string contains a substring, an array contains an element matching the argument, or an object contains a value matching the argument, and returns a boolean result.

```coffee
has_foo = thing.contains("foo")

# In:  {"thing":["this","foo","that"]}
# Out: {"has_foo":true}

# In:  {"thing":["this","bar","that"]}
# Out: {"has_foo":false}

# In:  {"thing":"this foo that"}
# Out: {"has_foo":true}

# In:  {"thing":"this bar that"}
# Out: {"has_foo":false}
```

### `enumerated`

Converts an array into a new array of objects, where each object has a field `index` containing the index of the element and a field `value` containing the original value of the element.

```coffee
foo = foo.enumerated()

# In:  {"foo":["bar","baz"]}
# Out: {"foo":[{"index":0,"value":"bar"},{"index":1,"value":"baz"}]}
```

### `exists`

Checks that a field, identified via a [dot path][field_paths], exists in an object.

```coffee
result = foo.exists("bar.baz")

# In:  {"foo":{"bar":{"baz":"yep, I exist"}}}
# Out: {"result":true}

# In:  {"foo":{"bar":{}}}
# Out: {"result":false}

# In:  {"foo":{}}
# Out: {"result":false}
```

### `fold`

Takes two arguments: an initial value, and a mapping function. For each element of an array the mapping receives an object with two fields `tally` and `value`. Where `tally` contains the current accumulated value and `value` is the value of the current element. The mapping must return the result of adding the value to the tally.

The initial value is the value the accumulator will have on the first call.

```coffee
sum = foo.fold(0, tally + value)

# In:  {"foo":[3,8,11]}
# Out: {"sum":22}
```

```coffee
result = foo.fold("", "%v%v".format(tally, value))

# In:  {"foo":["hello ", "world"]}
# Out: {"result":"hello world"}
```

### `get`

Extract a field value, identified via a [dot path][field_paths], from an object.

```coffee
result = foo.get(target)

# In:  {"foo":{"bar":"from bar","baz":"from baz"},"target":"bar"}
# Out: {"result":"from bar"}

# In:  {"foo":{"bar":"from bar","baz":"from baz"},"target":"baz"}
# Out: {"result":"from baz"}
```

### `keys`

Returns the keys of an object as an array. The order of the resulting array will be random.

```coffee
foo_keys = foo.keys()

# In:  {"foo":{"bar":1,"baz":2}}
# Out: {"foo_keys":["foo","bar"]}
```

### `length`

Returns the length of a string, array or object.

```coffee
foo_len = foo.length()

# In:  {"foo":"hello world"}
# Out: {"foo_len":11}
```

### `map_each`

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

### `merge`

Merge a source object into an existing destination object. When a collision is found within the merged structures (both a source and destination object contain the same non-object keys) the result will be an array containing both values, where values that are already arrays will be expanded into the resulting array.

```coffee
root = this.apply("foo").merge(this.apply("bar"))
```

### `sort`

Attempts to sort the values of an array in increasing order. The type of all values must match in order for the ordering to be accurate. Supports strings, integers and float values.

```coffee
sorted = foo.sort()

# In:  {"foo":["bbb","ccc",aaa"]}
# Out: {"sorted":["aaa","bbb","ccc"]}
```

### `sum`

Sum the numerical values of an array.

```coffee
sum = foo.sum()

# In:  {"foo":[3,8,4]}
# Out: {"sum":15}
```

### `values`

Returns the values of an object as an array. The order of the resulting array will be random.

```coffee
foo_vals = foo.values()

# In:  {"foo":{"bar":1,"baz":2}}
# Out: {"foo_vals":[1,2]}
```

## String Stuff

### `format`

Use a value string as a format specifier in order to produce a new string, using any number of provided arguments.

```coffee
foo = "%s(%v): %v".format(name, age, fingers)

# In:  {"name":"lance","age":37,"fingers":13}
# Out: {"foo":"lance(37): 13"}
```

```coffee
foo = template.format(arg1, arg2)

# In:  {"template":"%s -> %s","arg1":"foo","arg2":"bar"}
# Out: {"foo":"foo -> bar"}
```

### `lowercase`

Convert a string value into lowercase.

```coffee
foo = foo.lowercase()
```

### `parse_json`

Attempts to parse a string as a JSON document and returns the result.

```coffee
doc = doc.parse_json()

# In:  {"doc":"{\"foo\":\"bar\"}"}
# Out: {"doc":{"foo":"bar"}}
```

### `re_match`

Checks whether a regular expression matches against any part of a string and returns a boolean.

```coffee
matches = value.re_match("[0-9]")

# In:  {"value":"there are 10 puppies"}
# Out: {"matches":true}

# In:  {"value":"there are ten puppies"}
# Out: {"matches":false}
```

### `re_replace`

Replaces all occurrences of the argument regular expression in a string with a value. Inside the value $ signs are interpreted as submatch expansions, e.g. `$1` represents the text of the first submatch.

```coffee
new_value = value.re_replace("ADD ([0-9]+)","+($1)")

# In:  {"value":"foo ADD 70"}
# Out: {"new_value":"foo +(70)"}
```

### `slice`

Extract a slice from a string value by specifying two indices, a low and high bound, which selects a half-open range that includes the first element, but excludes the last one. 

If the second index is omitted then it defaults to the length of the input string.

```coffee
beginning = value.slice(0, 3)
end = value.slice(8)

# In:  {"value":"foo bar baz"}
# Out: {"beginning":"foo","end":"baz"}
```

### `uppercase`

Convert a string value into uppercase.

```coffee
foo = foo.uppercase()
```

[field_paths]: /docs/configuration/field_paths