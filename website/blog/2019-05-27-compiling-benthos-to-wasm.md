---
title: "Compiling Benthos to Web Assembly"
author: "Ashley Jeffs"
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: "Don't worry about why"
keywords: [
	"benthos",
	"go",
	"golang",
	"web assembly",
	"wasm",
	"gowasm",
]
tags: [ "Benthos Lab" ]
---

Web assembly won't fix seasons 7 and 8, but it's still pretty cool. At a
[Meltwater hackathon](https://underthehood.meltwater.com/blog/2019/06/17/benthos-lab-a-case-study-of-hackathon-innovation/) I had a project in mind (details soon to
follow) that would benefit hugely from Benthos running directly in the browser.
I therefore set out to compile it in wasm, this is my short and sweet journey.

<!--truncate-->

## The Build

The first thing I did and the first thing you ought to do if you are targeting
wasm yourself is skim through [this section of the Go wiki][wasm-go-wiki].

In short, I wrote a Go file:

``` go
package main

import (
	"syscall/js"

	"github.com/Jeffail/benthos/lib/config"
	"gopkg.in/yaml.v3"
)

func normalise(this js.Value, args []js.Value) interface{} {
	var configStr string
	if len(args) > 0 {
		configStr = args[0].String()
	}

	conf := config.New()

	// Ignoring errors for brevity
	yaml.Unmarshal([]byte(configStr), &conf)

	sanit, _ := conf.Sanitised()
	sanitBytes, _ := yaml.Marshal(sanit)

	return string(sanitBytes)
}

func main() {
	c := make(chan struct{}, 0)
	js.Global().Set("benthosNormaliseConfig", js.FuncOf(normalise))
	<-c
}
```

And compiled it:

``` sh
GOOS=js GOARCH=wasm go build -o main.wasm
```

I was pretty sure that this would be the end of the road for me. Benthos uses a
vast swathe of dependencies for its various connectors and so I was sure that I
would be immobilised with errors. However, to my surprise there were only three
(formatted for brevity):

``` text
lib/util/disk/check.go:29:11: undefined: syscall.Statfs_t
github.com/edsrzf/mmap-go@v1.0.0/mmap.go:77:9: undefined: mmap
github.com/lib/pq@v1.0.0/conn.go:321:13: undefined: userCurrent
```

Which involved some calls for a buffer implementation using a memory-mapped file
library and the PostgreSQL driver for the SQL package. The errors themselves are
basically "this thing doesn't exist in Web Assembly", which usually means the
library has a feature behind build constraints but doesn't support wasm yet.

The solution for these problems in my case was as simple as to not to do the
call, and perhaps document that the feature doesn't work with a wasm build.

Obviously, we only want to disable these calls specifically when targeting wasm.
In Go that's easy, stick a cheeky
[build constraint on there][go-build-constraint]. Here's the actual commit:
[9903b3d5d8519fcf7ecbce94c336e7f054a75942][wasm-commit], note that you can't
just constrain the feature, you also need to add an empty stub that has the
opposite constraint in order to satisfy your build.

## Executing Go From JavaScript

The [Go Wiki][wasm-go-wiki] shows you how to actually execute your wasm build
and I won't repeat it here, but I followed the steps and it was pretty straight
forward.

There was, however, one issue I came across. Some functions that I was calling
from JavaScript were causing my wasm runtime to panic and stop. The functions
all had channel blocking in common, something like this:

``` go
func ashHasACoolBlog(this js.Value, args []js.Value) interface{} {
	someChan <- args[0].String()
	return <-someOtherChanIHateNamingThings
}
```

The function would sometimes execute successfully. Other times, specifically for
longer running calls, I would get a deadlock panic:

``` text
fatal error: all goroutines are asleep - deadlock! wasm_exec.js:47:6
wasm_exec.js:47:6
goroutine 1 [chan receive]: wasm_exec.js:47:6
main.main() wasm_exec.js:47:6
	/home/ash/tmp/wasm/main.go:20 +0x7
```

Which was odd as they would be occasions where I would not expect a real
deadlock. I then found the relevant docs in the [`syscall/js`][syscall-js-func]
package:

> Blocking operations in the wrapped function will block the event loop. As a
> consequence, if one wrapped function blocks, other wrapped funcs will not be
> processed. A blocking function should therefore explicitly start a new
> goroutine.

The consequences of blocking sound pretty harmless here, but in reality it
seemed to be the cause of my deadlock crash. I assume the odd error message is a
result of some nuanced mechanics within the wasm runtime.

I didn't investigate this crash any further as I was a lazy idiot back in those
dark days. I simply stopped writing blocking functions, and instead spawned
goroutines everywhere like they were losers at a Nickelback concert:

``` go
func iJustWantToClarify(this js.Value, args []js.Value) interface{} {
	go func() {
		someChan <- args[0].String()
		otherThing := <-someOtherChanIHateNamingThings

		js.Global().Get("thatActually").Set(
			"textContent",
			"I quite enjoy and respect Knickelback as artists... " + otherThing,
		)
	}()
	return nil
}
```

## Other Issues

There weren't any. 

## Final Words

It took a day for me to get a working application together and soon I'll be
blogging about the resulting product. Web assembly with Go is dope.

Kudos to both the W3C and the Go team for taking their time to build something
to completion without rushing the conclusion. Yes, I'm still bitter about Game
of Thrones.

[meltwater]: https://underthehood.meltwater.com/blog/2019/06/17/benthos-lab-a-case-study-of-hackathon-innovation/
[Benthos]: https://www.benthos.dev/
[wasm-go-wiki]: https://github.com/golang/go/wiki/WebAssembly
[syscall-js-func]: https://godoc.org/syscall/js#Func
[go-build-constraint]: https://golang.org/pkg/go/build/#hdr-Build_Constraints
[wasm-commit]: https://github.com/Jeffail/benthos/commit/9903b3d5d8519fcf7ecbce94c336e7f054a75942#diff-146b6fd87106d7f70f56facf7b1e7d98