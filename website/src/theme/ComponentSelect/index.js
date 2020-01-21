import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

import classnames from 'classnames';

import styles from './styles.module.css';

function ComponentSelect(props) {
  let {type, singular} = props;
  const context = useDocusaurusContext();
  const names = context.siteConfig.customFields.components[type];

  if ( typeof(singular) !== 'string' ) {
    singular = type;
    if (/s$/.test(singular)) {
      singular = type.slice(0, -1);
    }
  }

  return <div class="dropdown dropdown--hoverable">
    <button class="button button--outline button--primary">Jump to {singular}</button>
    <ul className={classnames(styles.componentList, "dropdown__menu")}>
      {names.map((name) => {
        return <li>
          <a class="dropdown__link" href={`/docs/components/${type}/${name}`}>{name}</a>
        </li>
      })}
    </ul>
  </div>
}

export default ComponentSelect;