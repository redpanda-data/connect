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
map formating {
  root = value
  foo = pattern.format(value.foo)
  bar = pattern.format(value.bar)
}

first = {"value":doc,"pattern":"(%v)"}.apply("formating")
second = {"value":doc,"pattern":"[%v]"}.apply("formating")

# In:  {"doc":{"foo":"f","bar":"b","baz":"bz"}}
# Out: {"first":{"bar":"(b)","baz":"bz","foo":"(f)"},"second":{"bar":"[b]","baz":"bz","foo":"[f]"}}
```