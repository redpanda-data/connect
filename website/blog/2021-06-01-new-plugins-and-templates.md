---
title: Go Plugins V2 and Config Templates
author: Ashley Jeffs
author_url: https://github.com/Jeffail
author_image_url: /img/ash.jpg
description: It's ready, now we need guinea pigs
keywords: [
    "go",
    "golang",
    "stream processor",
    "ETL",
]
tags: [ "v4", "plugins", "templates", "roadmap" ]
---

I need help, attention and your sweet loving, and therefore It's time for a development update. Around five months ago I posted a [roadmap for Benthos v4](/blog/2021/01/04/v4-roadmap) that included some utterly unattainable goals that only a super human could achieve.

Now that most of those features are ready to beta test, namely a new plugins API and config templating, I'm looking for people to try them out and give feedback.

<!--truncate-->

## Config Templates

The new config templates functionality allows you to define parameterised templates for Benthos configuration snippets that can be imported with a cli flag and used in Benthos configs like native Benthos components.

This is going to be super useful in situations where you have commonly used configuration patterns but with small differences that prevent you from using the exact same config as a resource.

TODO EXAMPLE

To find out more configuration templates, including how to try them out, check out [the new templates page][configuration.templating].

## The V2 Go Plugins API

Benthos has had Go plugins for a while now

## Join the Community

I've been babbling on for months, so if this stuff is news to you then you're clearly out of the loop. Worry not, for you can remedy the situation by joining one or more of our [glorious community spaces][community].

[community]: /community
[configuration.templating]: /docs/configuration/templating
[plugins-feedback-thread]: https://github.com/Jeffail/benthos/discussions/754
[templates-feedback-thread]: https://github.com/Jeffail/benthos/discussions/755