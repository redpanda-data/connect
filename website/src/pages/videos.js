import React from 'react';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './videos.module.css';

function Videos() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;
  return (
    <Layout
      title={`${siteConfig.title} Videos`}
      description="A collection of Benthos videos">
      <header>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--6 col--offset-3')}>
              <h1 className={styles.videosTitle}>Benthos Videos</h1>
              <p>Rated U for Underwhelming.</p>
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className={styles.videoContainer}>
          <iframe width="560" height="315" src="https://www.youtube-nocookie.com/embed/videoseries?list=PL9hWaP-BQh2rvNuM29bTLlL0hYk6cqyT5" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
        </div>
      </main>
    </Layout>
  );
}

export default Videos;
