import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

import classnames from 'classnames';

import styles from './styles.module.css';

function ComponentSelect(props) {
  let {type, singular} = props;
  const context = useDocusaurusContext();
  const types = context.siteConfig.customFields.components[type];

  if ( typeof(singular) !== 'string' ) {
    singular = type;
    if (/s$/.test(singular)) {
      singular = type.slice(0, -1);
    }
  }

  return <div className="dropdown dropdown--hoverable">
    <button className="button button--outline button--primary">Jump to {singular}</button>
    <ul className={classnames(styles.componentList, "dropdown__menu")}>
      {types.map((info) => {
        return <li key={info.name}>
          <a className="dropdown__link" href={`/docs/components/${type}/${info.name}`}>{info.name}</a>
        </li>
      })}
    </ul>
  </div>
}

export default ComponentSelect;