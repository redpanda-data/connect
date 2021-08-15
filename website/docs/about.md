---
title: What is Benthos for?
sidebar_label: About
hide_title: false
---

<div style={{textAlign: 'center'}}><img src="/img/what-is-blob.svg" /></div>

Benthos is a declarative data streaming service that solves a wide range of data engineering problems with simple, chained, stateless [processing steps][docs.processors]. It implements transaction based resiliency with back pressure, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery without needing to persist messages during transit.

import ReactPlayer from 'react-player/youtube';

<div className='container margin-vert--lg'>
  <div className='row row--no-gutters'>
    <ReactPlayer
        className='col'
        height='300px'
        url='https://www.youtube.com/embed/88DSzCFV4Ng'
        controls={true}
    />
  </div>
</div>

It's [simple to deploy][docs.guides.getting_started], comes with a wide range of [connectors](#components), and is totally data agnostic, making it easy to drop into your existing infrastructure. Benthos has functionality that overlaps with integration frameworks, log aggregators and ETL workflow engines, and can therefore be used to complement these traditional data engineering tools or act as a simpler alternative.

Benthos is ready to commit to this relationship, are you?

import Link from '@docusaurus/Link';

<Link to="/docs/guides/getting_started" className="button button--lg button--outline button--block button--primary">Get Started</Link>

## Components

import ComponentsByCategory from '@theme/ComponentsByCategory';

### Inputs

<ComponentsByCategory type="inputs"></ComponentsByCategory>

---

### Processors

<ComponentsByCategory type="processors"></ComponentsByCategory>

---

### Outputs

<ComponentsByCategory type="outputs"></ComponentsByCategory>

[guides]: /cookbooks
[docs.guides.getting_started]: /docs/guides/getting_started
[docs.processors]: /docs/components/processors/about
