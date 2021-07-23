---
title: Introducing Benthos Lab
author: "Ashley Jeffs"
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: "Where you can build your very own monstrosities"
keywords: [
    "benthos",
    "benthos lab",
    "web assembly",
    "wasm",
    "go",
    "golang",
    "stream processor",
]
tags: [ "Benthos Lab" ]
---

After experimenting with innovative new ways to choke your browser to death I am
pleased to announce Benthos Lab, which lives at
[https://lab.benthos.dev](https://lab.benthos.dev).

<!--truncate-->

Benthos Lab is a website where users of the [Benthos stream processor][benthos]
can write, format, execute and share their pipeline configurations. This was
made possible by compiling the entire service (written in Go) into Web Assembly
so that it can run directly in your browser.

Here's a video of it in action: [https://youtu.be/1ZN-42A0sJU][lab-video].

Some technical details about how this was achieved can be found in
[a previous post of mine][wasm-blog]. The repo can be found at:
[https://github.com/benthosdev/benthos-lab][lab-repo], feel free to clone it,
hack it and host your own version.

[![benthos-lab](/img/introducing-benthos-lab/banner.svg)][benthos-lab]

Like every good laboratory it is scrappy and sometimes explodes. You need a
modern browser version and it doesn't currently support mobile. Regardless, I
think it's a good time to invite you try it out. Don't worry, I'll be watching
from a safe distance.

Also, if it does break on you then please remember that you don't pay me for
this, you vile parasite.

## Why

At Meltwater we have many distributed teams using Benthos for a wide and ever
increasing list of use cases, which results in a lot of remote collaboration.

For the slower moving, asynchronous types of work we are usually fine with ye
olde git. However, sometimes it's nice to quickly hash stuff out, at which point
things would suddenly devolve into a barrage of files getting chaotically thrown
at a slack channel.

![slamming-slack](/img/introducing-benthos-lab/slamslack.jpg)

By contrast, the lab is a civilised place where one can quickly and easily
create a pipeline concept, including sample input data, and share it directly
with a group. Opening up a shared lab provides all that same context, allows you
to make your own revisions to it, and even lets you execute it in order to see
the results it produces.

![genteel-lab](/img/introducing-benthos-lab/genteel.jpg)

For our teams this app dramatically reduced the time taken to create, prove and
demonstrate a pipeline concept. It's also much quicker for me to help teams that
have issues with their configs as they can make sure I have all the information
needed up front with a single URL.

### Use it to build your own web app

There's also plenty of unintended use cases for Benthos Lab as it basically
allows you to build your own custom web applications. Here's a session that
lower cases and normalises a JSON document, computes its sha256 hash and then
hex encodes the hash:
[https://lab.benthos.dev/l/N-3sss3WPjj#input](https://lab.benthos.dev/l/N-3sss3WPjj#input).

Pro tip: if you add the anchor `#input` to the end of the session URL then it
opens up at the input tab for quickly inserting stuff.

Since there's such a vast catalogue of Benthos
[processors available][benthos-procs] I've already found myself bookmarking a
few lab sessions as general utilities.

## Next Steps

With this running within the sandbox of your browser there's lots of missing
functionality. For example, you can't create TCP/UDP connections (as of right
now) and you can't access a file system.

However, I don't plan to address this any time soon as the intention of the app
is to test snippets of config, not to execute your whole damn streaming
pipeline. You ought to learn to manage your expectations.

What I will do though is continue to improve the UI. If a feature you want is
missing (or broken, obviously) then please [open an issue][lab-issues].

[benthos-lab]: https://lab.benthos.dev
[lab-video]: https://youtu.be/1ZN-42A0sJU
[wasm-blog]: /blog/2019/05/27/compiling-benthos-to-wasm/
[lab-repo]: https://github.com/benthosdev/benthos-lab
[lab-issues]: https://github.com/benthosdev/benthos-lab/issues
[benthos]: https://www.benthos.dev
[under-the-hood]: https://underthehood.meltwater.com/
[benthos-procs]: https://benthos.dev/docs/components/processors/about