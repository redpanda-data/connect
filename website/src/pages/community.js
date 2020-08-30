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
        </div>
      </header>
      <main>
        <div className="container">
          <div className="row margin-vert--lg">
            <div className="col text--center">
              <a href="https://gitter.im/jeffail-benthos/community" target="_blank"  className={styles.button}>
                <div className={classnames(styles.icon, styles.chat)}>
                </div>
                <div className={styles.title}>Chat</div>
                <div className={styles.description}>Join the Gitter channel</div>
              </a>
            </div>
            <div className="col text--center">
              <a href="https://twitter.com/jeffail" target="_blank" className={styles.button}>
                <div className={classnames(styles.icon, styles.twitter)}>
                </div>
                <div className={styles.title}>@jeffail</div>
                <div className={styles.description}>
                  Aggressively &#64;mention Ash
                </div>
              </a>
            </div>
            <div className="col text--center">
              <a href="mailto:hello@benthos.dev" className={styles.button}>
                <div className={classnames(styles.icon, styles.email)}>
                </div>
                <div className={styles.title}>hello@benthos.dev</div>
                <div className={styles.description}>Say hello via email</div>
              </a>
            </div>
          </div>
          <div className="row margin-vert--lg">
            <div className="col text--center">
              <p>You can also find us on the <a href="https://app.slack.com/client/T029RQSE6/CLWCBK7FY">#benthos channel on the gophers slack</a>. You can get an invite from <a href="https://gophersinvite.herokuapp.com/">this link</a>.</p>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Community;