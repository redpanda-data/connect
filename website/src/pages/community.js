import React from 'react';

import styles from './community.module.css';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Community() {
  const context = useDocusaurusContext();

  return (
    <Layout title="Community" description="Where to ask questions and find your soul mate">
      <header className="hero">
        <div className="container text--center">
          <h1>Community</h1>
          <div className="hero--subtitle">
            These are places where you can ask questions and find your soul mate (no promises).
          </div>
          <img className={styles.headerImg} src="/img/Blobsocial.svg" />
        </div>
      </header>
      <main>
        <div className="container">
          <div className="row margin-vert--lg">
            <div className="col text--center padding-vert--md">
              <div class="card">
                <div class="card__header">
                  <i className={classnames(styles.icon, styles.discord)}></i>
                </div>
                <div class="card__body">
                  <p>Join the official Benthos discord server</p>
                </div>
                <div class="card__footer">
                  <Link to="https://discord.gg/6VaWjzP" class="button button--outline button--primary button--block">Join</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div class="card">
                <div class="card__header">
                  <i className={classnames(styles.icon, styles.twitter)}></i>
                </div>
                <div class="card__body">
                  <p>Aggressively &#64;mention Ash on Twitter</p>
                </div>
                <div class="card__footer">
                  <Link to="https://twitter.com/jeffail" class="button button--outline button--primary button--block">Follow &#64;Jeffail</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div class="card">
                <div class="card__header">
                  <i className={classnames(styles.icon, styles.email)}></i>
                </div>
                <div class="card__body">
                  <p>Say hello via email</p>
                </div>
                <div class="card__footer">
                  <Link to="mailto:hello@benthos.dev" class="button button--outline button--primary button--block">hello&#64;benthos.dev</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div class="card">
                <div class="card__header">
                  <i className={classnames(styles.icon, styles.slack)}></i>
                </div>
                <div class="card__body">
                  <p>Join us on the &#35;benthos channel in the Gophers slack</p>
                </div>
                <div class="card__footer">
                  <Link to="https://invite.slack.golangbridge.org/" class="button button--outline button--success button--block">Get an invite</Link>
                </div>
                <div class="card__footer">
                  <Link to="https://app.slack.com/client/T029RQSE6/CLWCBK7FY" class="button button--outline button--primary button--block">Open</Link>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Community;