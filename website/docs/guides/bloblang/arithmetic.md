---
title: Bloblang Arithmetic
sidebar_label: Arithmetic
description: How arithmetic works within Bloblang
---

Bloblang supports a range of comparison operators `!`, `>`, `>=`, `==`, `<`, `<=`, `&&`, `||` and mathematical operators `+`, `-`, `*`, `/`, `%`. How these operators behave is dependent on the type of the values they're used with, and therefore it's worth fully understanding these behaviors if you intend to use them heavily in your mappings.

## Mathematical

All mathematical operators (`+`, `-`, `*`, `/`, `%`) are valid against number values, and addition (`+`) is also supported when both the left and right hand side arguments are strings. If a mathematical operator is used with an argument that is non-numeric (with the aforementioned string exception) then a [recoverable mapping error will be thrown][blobl.error_handling].

### Number Degradation

In Bloblang any number resulting from a method, function or arithmetic is either a 64-bit signed integer or a 64-bit floating point value. Numbers from input documents can be any combination of size and be signed or unsigned.

When a mathematical operation is performed with two or more integer values Bloblang will create an integer result, with the exception of division. However, if any number within a mathematical operation is a floating point then the result will be a floating point value.

In order to explicitly coerce numbers into integer types you can use the [`.ceil()`, `.floor()`, or `.round()` methods][blobl.methods.number_manipulation].

## Comparison

The not (`!`) operator reverses the boolean value of the expression immediately following it, and is valid to place before any query that yields a boolean value. If the following expression yields a non-boolean value then a [recoverable mapping error will be thrown][blobl.error_handling].

If you wish to reverse the boolean result of a complex query then simply place the query within brackets (`!(this.foo > this.bar)`).

### Equality

The equality operators (`==` and `!=`) are valid to use against any value type. In order for arguments to be considered equal they must match in both their basic type (`string`, `number`, `null`, `bool`, etc) as well as their value. If you wish to compare mismatched value types then use [coercion methods][blobl.methods.type_coercion].

Number arguments are considered equal if their value is the same when represented the same way, which means their underlying representations (integer, float, etc) do not need to match in order for them to be considered equal.

### Numerical

Numerical comparisons (`>`, `>=`, `<`, `<=`) are valid to use against number values only. If a non-number value is used as an argument then a [recoverable mapping error will be thrown][blobl.error_handling].

### Boolean

Boolean comparison operators (`||`, `&&`) are valid to use against boolean values only (`true` or `false`). If a non-boolean value is used as an argument then a [recoverable mapping error will be thrown][blobl.error_handling].

[blobl.error_handling]: /docs/guides/bloblang/about#error-handling
[blobl.methods.number_manipulation]: /docs/guides/bloblang/methods#number-manipulation
[blobl.methods.type_coercion]: /docs/guides/bloblang/methods#type-coercion
