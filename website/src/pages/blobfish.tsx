import React from 'react';

import Layout from '@theme/Layout';

import classnames from 'classnames';
import styles from './blobfish.module.css';

const emojis = [
  "blob.png",
  "blobok.png",
  "blobyes.png",
  "blobno.png",
  "blobcool.png",
  "blobkiss.png",
  "blobthanks.png",
  "blobthinking.png",
  "blobpalm.png",
  "blobwave.png",
  "blobheart.png",
  "blobcrying.png",
  "blobcrylaugh.png",
  "blobnaughty.png",
  "blobmad.png",
  "blobbot.png",
  "blobemo.png",
  "blobnerd.png",
  "cowblob.png",
  "blobbug.png",
  "blobpirate.png",
  "blobbounce.gif",
  "blobtrance.gif",
];

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
            <div className="row">
              <div className={classnames("margin-bottom--lg", "col", "col--8", "col--offset-2", styles.emojiContainer)}>
                {emojis.map((emoji, idx) => (
                  <a key={idx} href={`/img/emojis/${emoji}`}>
                    <img className={styles.emoji} src={`/img/emojis/${emoji}`}/>
                  </a>
                ))}
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Blobfish;