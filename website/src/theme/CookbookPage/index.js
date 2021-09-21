import React from 'react';

import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import MDXComponents from '@theme/MDXComponents';
import {MDXProvider} from '@mdx-js/react';
import TOC from '@theme/TOC';

import classnames from 'classnames';
import styles from './styles.module.css';

function CookbookPage(props) {
  const {content: CookbookContents} = props;
  const {frontMatter, metadata} = CookbookContents;
  const {title} = frontMatter;
  const {keywords} = metadata;

  return (
    <Layout title={metadata.title} description={metadata.description} keywords={keywords}>
      <div className="container">
        <div className="row">
          <div className="col">
            <article className={styles.cookbookContainer}>
              <header className={classnames(styles.header, 'margin-top--xl')}>
                <h1 className={styles.cookbookTitle}>{title}</h1>
                <p className={styles.cookbookDescription}>{metadata.description}</p>
              </header>
              <div className="container container--narrow container--bleed margin-vert--xl">
                <section className="markdown">
                  <MDXProvider components={MDXComponents}><CookbookContents /></MDXProvider>
                </section>
                <Link to="/cookbooks" className="button button--outline button--primary margin-vert--lg">Find more cookbooks</Link>
              </div>
            </article>
          </div>
          {CookbookContents.toc && (
            <div className="col col--2">
              <TOC toc={CookbookContents.toc} />
            </div>
          )}
        </div>
      </div>
    </Layout>
  );
}

export default CookbookPage;
