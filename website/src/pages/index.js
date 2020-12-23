import React from 'react';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';
import CodeSnippet from "@site/src/theme/CodeSnippet";
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

const installs = [
  {
    label: 'Curl',
    snippet: `# Install
curl -Lsf https://sh.benthos.dev | bash

# Make a config
benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
benthos -c ./config.yaml`
  },
  {
    label: 'Homebrew',
    snippet: `# Install
brew install benthos

# Make a config
benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
benthos -c ./config.yaml`
  },
  {
    label: 'Docker',
    snippet: `# Pull
docker pull jeffail/benthos

# Make a config
docker run --rm jeffail/benthos create nats/protobuf/aws_sqs > ./config.yaml

# Run
docker run --rm -v $(pwd)/config.yaml:/benthos.yaml jeffail/benthos`
  },
]

const snippets = [
  {
    label: 'Mapping',
    further: '/docs/guides/bloblang/about',
    config: `input:
  gcp_pubsub:
    project: foo
    subscription: bar

pipeline:
  processors:
    - bloblang: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()

output:
  redis_streams:
    url: tcp://TODO:6379
    stream: baz
    max_in_flight: 20`,
  },
  {
    label: 'Multiplexing',
    further: '/docs/components/outputs/about#multiplexing-outputs',
    config: `input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

output:
  switch:
    cases:
      - check: doc.tags.contains("AWS")
        output:
          aws_sqs:
            url: https://sqs.us-west-2.amazonaws.com/TODO/TODO
            max_in_flight: 20

      - output:
          redis_pubsub:
            url: tcp://TODO:6379
            channel: baz
            max_in_flight: 20`,
  },
  {
    label: 'Enrichments',
    further: '/cookbooks/enrichments',
    config: `input:
  mqtt:
    urls: [ tcp://TODO:1883 ]
    topics: [ foo ]

pipeline:
  processors:
    - branch:
        request_map: |
          root.id = this.doc.id
          root.content = this.doc.body
        processors:
          - aws_lambda:
              function: sentiment_analysis
        result_map: root.results.sentiment = this

output:
  aws_s3:
    bucket: TODO
    path: '\${! meta("partition") }/\${! timestamp_unix_nano() }.tar.gz'
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip`,
  },
];

function Snippet({label, config}) {
  return (
    <CodeSnippet className={styles.configSnippet} snippet={config}></CodeSnippet>
  );
}

const features = [
  {
    title: 'Takes Care of the Dull Stuff',
    imageUrl: 'img/Blobboring.svg',
    description: (
      <>
        <p>
          Benthos solves common data engineering tasks such as transformations, integrations, and multiplexing with declarative configuration. This allows you to easily and incrementally adapt your data pipelines as requirements change, letting you focus on the more exciting stuff.
        </p>
        <p>
          It comes armed with a wide range of <a href="/docs/components/processors/about">processors</a>, a <a href="/docs/guides/bloblang/about">lit mapping language</a>, stateless <a href="/docs/configuration/windowed_processing">windowed processing capabilities</a> and an <a href="/blobfish">industry leading mascot</a>.
        </p>
      </>
    ),
  },
  {
    title: 'Well Connected',
    imageUrl: 'img/Blobborg.svg',
    description: (
      <>
        <p>
          Benthos is able to glue a wide range of <a href="/docs/components/inputs/about">sources</a> and <a href="/docs/components/outputs/about">sinks</a> together and hook into a variety of <a href="/docs/components/processors/sql">databases</a>, <a href="/docs/components/processors/cache">caches</a>, <a href="/docs/components/processors/http">HTTP APIs</a>, <a href="/docs/components/processors/aws_lambda">lambdas</a> and <a href="/docs/components/processors/about">more</a>, enabling you to seamlessly drop it into your existing infrastructure.
        </p>
        <p>
          Working with disparate APIs and services can be a daunting task, doubly so in a streaming data context. With Benthos it's possible to break these tasks down and automatically parallelize them as <a href="/cookbooks/enrichments">a streaming workflow</a>.
        </p>
      </>
    ),
  },
  {
    title: 'Reliable and Scalable',
    imageUrl: 'img/Blobscales.svg',
    description: (
      <>
        <p>
          Benthos runs fast and processes messages using a transaction model, making it able to guarantee at-least-once delivery even in the event of crashes or unexpected server faults.
        </p>
        <p>
          It's completely stateless, allowing for easy deployment and liberal scaling. It also exposes <a href="/docs/components/metrics/about">metrics</a> and <a href="/docs/components/tracers/about">tracing</a> events to targets of your choice.
        </p>
        <p>
          At Meltwater it's enriching over 450 million documents per day with a network of more than 20 NLP services. It sounds very interesting but rest assured, <a href="https://underthehood.meltwater.com/blog/2019/08/26/enriching-450m-docs-daily-with-a-boring-stream-processor">it's totally drab</a>.
        </p>
      </>
    ),
  },
  {
    title: 'Extendable',
    imageUrl: 'img/Blobextended.svg',
    description: (
      <>
        <p>
          Sometimes the components that come with Benthos aren't enough. Luckily, Benthos has been designed to be easily plugged with whatever components you need.
        </p>
        <p>
          You can either write plugins <a href="https://github.com/benthosdev/benthos-plugin-example">directly in Go (recommended)</a> or you can have Benthos run your plugin as a <a href="/docs/components/processors/subprocess">subprocess</a>.
        </p>
      </>
    ),
  },
];

function Feature({imageUrl, title, description}) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={classnames('col col--6', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={classnames('padding-vert--md', styles.featureImage)} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="The stream processor for mundane tasks"
      keywords={["benthos","stream processor","data engineering","ETL","ELT","event processor","go","golang"]}>
      <header className={classnames('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--5 col--offset-1')}>
              <h1 className="hero__title">{siteConfig.title}</h1>
              <p className="hero__subtitle">{siteConfig.tagline}</p>
              <div className={styles.buttons}>
                <Link
                  className={classnames(
                    'button button--outline button--primary button--lg',
                    styles.getStarted,
                  )}
                  to={useBaseUrl('docs/guides/getting_started')}>
                  Get Started
                </Link>
              </div>
            </div>
            <div className={classnames('col col--5')}>
              <img className={styles.heroImg} src="img/logo_hero.svg" />
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container">
          <div className="row">
            <div className={classnames(`${styles.pitch} col col--6`)}>
              <h2>It's boringly easy to use</h2>
              <p>
                Written in Go, deployed as a static binary, declarative configuration. Cloud native as utter heck.
              </p>
              {installs && installs.length && (
                <Tabs defaultValue={installs[0].label} values={installs.map((props, idx) => {
                  return {label:props.label, value:props.label};
                })}>
                  {installs.map((props, idx) => (
                    <TabItem value={props.label}>
                      <CodeSnippet snippet={props.snippet} lang="bash"></CodeSnippet>
                    </TabItem>
                  ))}
                </Tabs>
              )}
            </div>
            <div className={classnames('col col--6')}>
                {snippets && snippets.length && (
                  <section className={styles.configSnippets}>
                    <Tabs defaultValue={snippets[0].label} values={snippets.map((props, idx) => {
                      return {label:props.label, value:props.label};
                    })}>
                      {snippets.map((props, idx) => (
                        <TabItem value={props.label}>
                          <>
                          <Snippet key={idx} {...props} />
                          <Link
                            className={classnames('button button--outline button--secondary')}
                            to={props.further}>
                            Read more
                          </Link>
                          </>
                        </TabItem>
                      ))}
                    </Tabs>
                  </section>
                )}
            </div>
          </div>
        </div>
        {features && features.length && (
          <section className={styles.features}>
            <div className="container margin-vert--md">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
        <section className={styles.loveSection}>
          <div className="container">
            <div className="row">
              <div className={classnames('col col--6')}>
                <h3>Sponsored by the following heroes</h3>
                <a href="https://www.meltwater.com/" className={styles.sponsorLink}><img className={styles.meltwaterImg} src="/img/sponsors/mw_logo.png" /></a>
                <a href="https://www.whiteops.com/" className={styles.sponsorLink}><img className={styles.whiteopsImg} src="/img/sponsors/whiteops_logo.png" /></a>
                <a href="https://www.infosum.com/" className={styles.sponsorLink}><img className={styles.infosumImg} src="/img/sponsors/infosum_logo.png" /></a>
              </div>
              <div className={classnames('col col--6', styles.loveSectionPlea)}>
                <div>
                  <a href="https://github.com/sponsors/Jeffail">
                    <img className={styles.loveImg} src="img/blobheart.svg" alt="Blob Heart" />
                  </a>
                </div>
                <Link
                  className={classnames('button button--danger')}
                  to="https://github.com/sponsors/Jeffail">
                  Become a sponsor
                </Link>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
