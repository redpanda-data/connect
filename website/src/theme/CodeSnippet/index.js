import React from 'react';

import styles from './styles.module.css';

import Highlight, { defaultProps } from "prism-react-renderer";
import monokai from "@site/src/plugins/prism_themes/monokai";

function CodeSnippet(props) {
  const {
    lang = 'yaml',
    snippet,
  } = props;

  return (
    <Highlight {...defaultProps} theme={monokai} code={snippet} language={lang}>
      {({ className, style, tokens, getLineProps, getTokenProps }) => (
        <pre className={`${className} ${styles.codeSnippet}`} style={style}>
          {tokens.map((line, i) => (
            <div {...getLineProps({ line, key: i })}>
              {line.map((token, key) => (
                <span {...getTokenProps({ token, key })} />
              ))}
            </div>
          ))}
        </pre>
      )}
    </Highlight>
  );
}

export default CodeSnippet;
