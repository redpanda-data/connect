import React from 'react';

import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

import classnames from 'classnames';
import styles from './support.module.css';

function Support() {
  return (
    <Layout title="Support" description="How to get Benthos support">
      <header className={classnames('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--5 col--offset-1')}>
              <h1 className="hero__title">Benthos Support</h1>
              <p className="hero__subtitle">When Benthos hands you lemons this page lists people to angrily throw them at.</p>
              <small>(That was just a metaphor - lemons not provided)</small>
            </div>
            <div className={classnames('col col--5')}>
              <img className={styles.heroImg} src="/img/blobheart.svg" />
            </div>
          </div>
        </div>
      </header>
      <main>
        <section>
          <div className="container container-narrow padding-top--lg">
            <div className="row margin-bottom--lg">
              <div className="col col--12">
                <p>
Benthos has a helpful and conveniently global community so if you have quick questions, are in the market for ideas, or just want to make some friends, it's worth trying your luck with our <a href="#community-support">community spaces</a>. However, for organisations that want quicker action, guaranteed attention or some one-on-one consulting time there are some <a href="#paid-services">paid services available</a> that are worth considering.
                </p>
              </div>
            </div>
          </div>
        </section>
        <section>
          <div className="container container-narrow padding-bottom--lg">
            <div className="row margin-bottom--lg">
              <div className="col col--12">
                <h2 id="paid-services">Paid Services
                  <a className="hash-link" href="#paid-services" title="Direct link to heading">​</a></h2>
                  <p>
Paid services options range from ad hoc consultations to long term help desk contracts. These can be tailored to your particular needs and are subject to availability. Please reach out to <Link to="mailto:business@benthos.dev">business&#64;benthos.dev</Link> to get started.
                  </p>
                  <p>
It's also worth noting that some subscription offerings of <a href="https://studio.benthos.dev">Benthos Studio</a> include support and may offer enough for your particular use case.
                  </p>
              </div>
            </div>
          </div>
        </section>
        <section>
          <div className="container container-narrow padding-bottom--lg">
            <div className="row margin-bottom--lg">
              <div className="col col--12">
                <h2 id="community-support">Community Support
                  <a className="hash-link" href="#community-support" title="Direct link to heading">​</a></h2>
                <p>
Nothing makes the Benthos community happier than welcoming new blobs into our blobosphere, and that includes answering questions, helping you to fit Benthos around your use cases, and general chit chat about the project, and there are <Link to="/community">multiple spaces where you can find us</Link>. However, we are a finite number of entities (for now) and there are limits to how much of our free time and energy we can realistically spend on these activities.
                </p>
                <p>
Keeping the community cogs turning is therefore a balancing act between encouraging shy users to seek help when they need it, and gently teaching others (the noisy ones) how to reduce their reliance on our help. Here's some tips on how to get the best of us.
                </p>
              </div>
            </div>
            <div className="row margin-bottom--lg">
              <div className="col col--12">
                <h2 id="before-asking-community">Before Asking for Help
                  <a className="hash-link" href="#before-asking-community" title="Direct link to heading">​</a></h2>
                <h3>Try the Documentation</h3>
                <p>
Give the <Link to="/docs/about">Benthos documentation a try</Link>, no one expects you to read the whole thing but it's there for a reason. Try using the search functionality and if there's a section that covers the area you're looking at then try your best to understand it. However, with that said, <strong>you will not be judged for missing something</strong>, if you're not seeing the answers you need then asking the community is definitely the reasonable thing to do.
                </p>
                <h3>Consider a Simpler Approach</h3>
                <p>
When you have a particular solution in mind it's easy to slip into a mindset that blocks you from considering other approaches even when it's not working out. If you're struggling to find out how to make your solution work with Benthos then take a deep breath, think about ponies or something for a few minutes, and consider if there's a more Benthosy way in which your problem could be solved.
                </p>
                <p>
If not then we still want to hear from you as maybe it's an interesting use case we can implement a proper solution for, but it's good to have considered other approaches before it gets to us.
                </p>
                <h3>Test Things Yourself</h3>
                <p>
Benthos has lots of ways to try solutions out, it has <Link to="/docs/configuration/unit_testing">unit tests</Link>, a <Link to="/docs/components/inputs/generate"><code>generate</code> input</Link>, a <code>benthos blobl server</code> subcommand, and many more ways to get some trial and error done before asking for help. It's frustrating for us to spend our time reading and understanding configuration files and use cases that make up part of your question when it could have been answered with a few seconds of testing.
                </p>
              </div>
            </div>
            <div className="row margin-bottom--lg">
              <div className="col col--12">
                <h2 id="when-asking-community">When Asking for Help
                  <a className="hash-link" href="#when-asking-community" title="Direct link to heading">​</a></h2>
                <h3>Focus on the Problem</h3>
                <p>
When asking us for help try and focus on the specific problem you have rather than the higher level details. Ideally we want to see things like concise examples of data you have versus what you want to get out, or a description of how you want data to flow through your pipeline. Please also reduce config examples down into the basics that are relevant to your question. Any details that you provide that <i>isn't</i> specifically part of the problem is only a hurdle that we will need to overcome before we can understand your question.
                </p>
                <p>
If your particular question requires knowledge of private and commercial applications that you have built or are building then the public support channels are not the right place to ask it.
                </p>
                <h3>Reproduce Your Issue</h3>
                <p>
It's common for users to point out an issue they're seemingly having with Benthos and it then turns out to be an unrelated issue with cached builds, configs not being saved, docker images not being pulled fresh, etc. These encounters may sound rare but anecdotally they occur more often in our support channels than actual bugs, and we've spent considerable measures of time trying to reproduce issues that don't exist (yet). By attempting to isolate and reproduce issues yourself you can significantly reduce the burden on us.
                </p>
                <h3>Provide Context</h3>
                <p>
Things that a typical community member cannot do include mind-reading, assuming control of your PC (I hope) and time travelling into the past in order to obtain context of the problem on your behalf. In order to work around these limitations please make sure that when you are asking us for help regarding a problem you've had that at the very least you are able to give us any error messages/logs that were emitted by Benthos when it happened.
                </p>
                <h3>Never Direct Message</h3>
                <p>
Unless you are explicitly instructed to do so please never direct message maintainers or community members for support, and avoid directly pinging them. If you aren't receiving responses in a public support channel it is because they currently do not have time to address your issue, please be patient or consider a <a href="#paid-services">paid support option instead</a>. Direct messages only serve to add extra pressure on volunteers, and answering questions via direct messages denies other users the opportunity to read the answers for themselves.
                </p>
                <p>
If there is sensitive information within your question that you do not want to expose publicly then please take the time to scrub that information from the material you are sharing.
                </p>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Support;