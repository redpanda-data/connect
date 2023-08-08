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
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.discord)}></i>
                </div>
                <div className="card__body">
                  <p>Join the official Benthos discord server</p>
                </div>
                <div className="card__footer">
                  <Link to="https://discord.gg/6VaWjzP" className="button button--outline button--primary button--block">Join</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.twitter)}></i>
                </div>
                <div className="card__body">
                  <p>Aggressively &#64;mention Ash on Twitter</p>
                </div>
                <div className="card__footer">
                  <Link to="https://twitter.com/jeffail" className="button button--outline button--primary button--block">Follow &#64;Jeffail</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.slack)}></i>
                </div>
                <div className="card__body">
                  <p>Join us on the &#35;benthos channel in the Gophers slack</p>
                </div>
                <div className="card__footer">
                  <Link to="https://invite.slack.golangbridge.org/" className="button button--outline button--success button--block">Get an invite</Link>
                </div>
                <div className="card__footer">
                  <Link to="https://app.slack.com/client/T029RQSE6/CLWCBK7FY" className="button button--outline button--primary button--block">Open</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.email)}></i>
                </div>
                <div className="card__body">
                  <p>For sensitive or security related queries pop us an email</p>
                </div>
                <div className="card__footer">
                  <Link to="mailto:hello@benthos.dev" className="button button--outline button--primary button--block">hello&#64;benthos.dev</Link>
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