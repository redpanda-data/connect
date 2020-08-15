import React from 'react';

import Layout from '@theme/Layout';

import classnames from 'classnames';
import styles from './blobfish.module.css';

function Blobfish() {
  return (
    <Layout title="Blobfish" description="The offical Benthos mascot">
      <header className={classnames("hero")}>
        <div className="container">
          <div className="row">
            <div className="col col--6 col--offset-3 text--center">
              <img className={styles.headerImg} src="/img/logo.svg" />
              <div>
                <h1>The Benthos Blobfish</h1>
                <div className="hero--subtitle">
                    The official Benthos mascot.
                </div>
              </div>
            </div>
          </div>
        </div>
      </header>
      <main>
        <section>
          <div className="container container-narrow padding-top--lg">
            <div className="row margin-bottom--lg">
              <div className="col col--8 col--offset-2">
                <p>
The Benthos blobfish began its life as a dumb placeholder logo, and since nothing has changed this is still the case.
                </p>
                <p>
Variations of the mascot such as the <a href="/img/Blobpirate.svg">dapper captain blobfish</a> and the utterly flawless <a href="/img/Blobchef.svg">chef blobish</a> were created with love by <a href="https://biscuittowndesigns.com/">Esther Arzola</a>. Sticker packs and other swag can be purchased directly from <a href="https://www.redbubble.com/people/earzola/shop">https://www.redbubble.com/people/earzola/shop</a>.
                </p>
              </div>
            </div>
          </div>
        </section>
        <section>
          <div className="container container--xs">
            <div className="row">
              <div className="col text--center">
                <h3 className="margin-bottom--sm">Emojis</h3>
                <p>Use these with great care.</p>
              </div>
            </div>
            <div className="row margin-bottom--lg">
              <div className="col text--center">
                <a href="/img/emojis/blob.png">
                  <img className={styles.emoji} src="/img/emojis/blob.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobok.png">
                  <img className={styles.emoji} src="/img/emojis/blobok.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobyes.png">
                  <img className={styles.emoji} src="/img/emojis/blobyes.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobno.png">
                  <img className={styles.emoji} src="/img/emojis/blobno.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobcool.png">
                  <img className={styles.emoji} src="/img/emojis/blobcool.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobkiss.png">
                  <img className={styles.emoji} src="/img/emojis/blobkiss.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobthinking.png">
                  <img className={styles.emoji} src="/img/emojis/blobthinking.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobpalm.png">
                  <img className={styles.emoji} src="/img/emojis/blobpalm.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobheart.png">
                  <img className={styles.emoji} src="/img/emojis/blobheart.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobcrying.png">
                  <img className={styles.emoji} src="/img/emojis/blobcrying.png"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobbounce.gif">
                  <img className={styles.emoji} src="/img/emojis/blobbounce.gif"/>
                </a>
              </div>
              <div className="col text--center">
                <a href="/img/emojis/blobtrance.gif">
                  <img className={styles.emoji} src="/img/emojis/blobtrance.gif"/>
                </a>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Blobfish;