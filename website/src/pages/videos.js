import React from 'react';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './videos.module.css';
import ReactPlayer from 'react-player/youtube'

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
              <p>All videos are rated U for Underwhelming. If you'd rather access these videos directly from YouTube you can find them on <a href="https://www.youtube.com/channel/UCjIYEhBrw3GQwpRWe1asufg">the Jeffail channel</a>.</p>
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container margin-vert--lg">
          <div className="row">
            <div className="col col--8 col--offset-2">
              <h2>Tutorials</h2>
              <p>This playlist contains tutorial videos covering various aspects of Benthos use.</p>
            </div>
            <ReactPlayer
              className={classnames('col col--8 col--offset-2')}
              url='https://www.youtube-nocookie.com/embed/videoseries?list=PL9hWaP-BQh2rvNuM29bTLlL0hYk6cqyT5'
              controls='true'
            />
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Videos;
