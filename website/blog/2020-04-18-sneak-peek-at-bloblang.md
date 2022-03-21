---
title: Sneak Peek at Bloblang
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: An experiment in mapping languages
keywords: [
    "benthos",
    "bloblang",
    "go",
    "golang",
    "stream processor",
    "mapping",
]
tags: [ "Bloblang" ]
---

For the last few weekends I've been dipping my toes in a mapping language design that I'm calling Bloblang. Bloblang is specifically designed for data queries and (eventually) structural data mappings. In Benthos version 3.12, which I'm planning to release today, you can play around with a limited feature set of Bloblang by using it in [function interpolations](/docs/configuration/interpolation).

<!--truncate-->

## Why

My life has no meaning. Also, mapping is one of the most common boring tasks in stream and event processing. Given Benthos is meant to specialise in the boring and mundane it makes sense to treat mapping as a first class citizen.

Up until now the story for mapping documents in Benthos has been to use [JMESPath][processor.jmespath], [AWK][processor.awk] or a string of the general purpose JSON processors. Time and time again it has been made apparent that it ain't good enough for many use cases.

I should mention at this point that there's also the option of [IDML][idml], and although Benthos hasn't supported it internally there is a solution to [running it in your pipeline][processor.subprocess].

For the last few years I've been helping users adopt these options and each time they fall short I've taken note of where the gaps are. This is an important part of the "research" phase for a language, but I also don't want to dwell on it. Here's an insultingly terse summary of what we currently have within Benthos.

### JMESPath

The spiritual cousin of [jq][jq], [JMESPath][jmespath] is a great spec for mapping JSON documents, especially so when your intention is to outright replace the original document.

However, when our goal is to preserve the majority of the existing document, and we only wish to express isolated mutations within the structure, it becomes ugly and risky. For example, changing just `foo.bar.baz` to `this value` looks like this:

```
merge(@, {
  "foo": merge(foo, {
	  "bar": merge(bar, {
	    "baz": "this value"
	  })
  })
})
```

Hopefully you don't add a typo there or miss on a `merge`, otherwise you're scrapping a large chunk of your original document!

Expressing your entire map in one single object also scales pretty poorly as the mapping grows in complexity.

A final and Benthos specific issue is that JMESPath only supports mapping the content of Benthos messages, without the ability to modify or reference the metadata of a message or other messages of a batch, which would be great for [windowed processing][windowed-processing].

### AWK

Benthos has an [AWK processor][processor.awk], and since this is a proper programming language it has uses far beyond mapping. However, this also makes it riskier to use for large and complex maps. More opportunities to write bugs, more opportunities to break your program, more opportunities to regress.

A simpler language specifically designed for mappings is a much more scalable solution as it reduces the opportunities for mistakes as both maps and teams grow. Although, risk aside, the major problem with using AWK within Benthos is the performance hit.

### JSON Processor

The JSON processor is pretty flexible and would be the highest performer of all options here. However, beyond one or two mutations a mapping becomes an absolute mess of YAML, and if we need to add conditional maps into the mix it becomes much worse.

It has been clear to me for a while that this processor is so quickly and easily outgrown by a typical user config that it perhaps ought to be entirely replaced with a real mapping solution.

### IDML

If I could run [IDML][idml] natively from Benthos then Bloblang wouldn't be happening. In my opinion [IDML][idml] is a criminally underused technology and absolutely nails the issue of mapping data at scale.

Similar to JMESPath the language itself doesn't have a concept of metadata, or querying across multiple documents (a batch). The issue I had here was that if I were going to go through the trouble of implementing IDML in Go I might as well add metadata and cross-batch querying, making it a different language anyway.

However, I'm definitely writing Bloblang with IDML in mind, and if I manage to reach feature parity with IDML then I intend to break it out into its own lib and offer it to the org, with my Bloblang extensions as Benthos specific plugins.

## Features

So with that in mind what does Bloblang look like? Right now we only have queries, which is the "right hand side" of a mapping. These queries support literals:

```
"string literal"
true
93435.45
```

And arithmetic:

```
50 + 34
("this" == "that") || ("that" == "that")
```

And functions:

```
json("foo.bar.baz")
meta("kafka_key")
timestamp_unix()
```

And methods, which are attached to a function or value:

```
json("foo.bar.baz").from_all().sum()
```

And path literals with coalescing:

```
json().foo.(bar | something_else).baz
```

## Next Steps

In terms of core syntaxes Bloblang is basically complete. It's implemented using parser combinators, and is very easy for me to extend with new functions and methods. Soon I'll expand Bloblang to support left hand query targets, which is when it really becomes a mapping language. It'll look something like this:

```yaml
pipeline:
  processors:
  - bloblang:
      mapping: |
        json.foo.bar = json().(something + another.thing)
        json.and_this = meta("kafka_key").base64()
```

And I'll also add a `condition` type for expressing logic as a Bloblang query:

```yaml
pipeline:
  processors:
  - filter_parts:
      bloblang:
        query: |
          (meta("kafka_topic") == "junk") &&
            json().foo.(bar | baz.quz).id.contains("blah")
```

Until I'm allowed to practice with my professional rock paper scissors team again I'm sure each weekend will deliver something new to the world of Bloblang.

[function-interpolations]: /docs/configuration/interpolation
[windowed-processing]: /docs/configuration/windowed_processing
[processor.jmespath]: /docs/components/processors/jmespath
[processor.awk]: /docs/components/processors/awk
[idml]: https://idml.io/
[processor.subprocess]: /docs/components/processors/subprocess
[jq]: https://stedolan.github.io/jq/
[jmespath]: https://jmespath.org/
