---
title: What is Benthos for?
sidebar_label: About
hide_title: false
---

<div style={{textAlign: 'center'}}><img src="/img/what-is-blob.svg" /></div>

Benthos is a stream processor that solves seemingly complex tasks with simple,
chained, stateless [processing steps][docs.processors]. Doing so makes
understanding, deploying and maintaining your pipelines easier and gives you
full control of your system.

Benthos implements transaction based resiliency with back pressure, so when
connecting to at-least-once sources and sinks it's able to guarantee
at-least-once delivery without needing to persist messages during transit.
This, combined with the wide range of [inputs][docs.inputs], [processors][docs.processors]
and [outputs][docs.outputs] that it offers makes it a rather tasty dish for
processing important data (or junk data as well actually).

Benthos is ready to commit to this relationship, are you?

import Link from '@docusaurus/Link';

<Link to="/docs/guides/getting_started" className="button button--outline button--primary">Get Started</Link>

[guides]: /cookbooks
[docs.inputs]: /docs/components/inputs/about
[docs.processors]: /docs/components/processors/about
[docs.outputs]: /docs/components/outputs/about