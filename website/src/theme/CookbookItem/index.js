import React from 'react';

import Link from '@docusaurus/Link';

import classnames from 'classnames';

import styles from './styles.module.css';

function CookbookItem(props) {
  const {
    frontMatter,
    metadata,
  } = props;
  const {description, permalink} = metadata;
  const {title} = frontMatter;

  return (
    <div>
      <Link to={permalink} className={classnames(styles.cookbookPostItem)}>
        <article>
          <h2>{title}</h2>
          <div>{description}</div>
        </article>
      </Link>
    </div>
  );
}

export default CookbookItem;
