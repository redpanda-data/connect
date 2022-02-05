/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, {useEffect, useState, useRef} from 'react';
import clsx from 'clsx';
import Highlight, {defaultProps} from 'prism-react-renderer';
import copy from 'copy-text-to-clipboard';
import {usePrismTheme} from '@docusaurus/theme-common';
import Translate, {translate} from '@docusaurus/Translate';

import styles from './styles.module.css';

import {useThemeConfig} from '@docusaurus/theme-common';

export default function CodeBlock({
  children,
  className: languageClassName,
}) {
  const {prism} = useThemeConfig();

  const [showCopied, setShowCopied] = useState(false);
  const [mounted, setMounted] = useState(false);
  // The Prism theme on SSR is always the default theme but the site theme
  // can be in a different mode. React hydration doesn't update DOM styles
  // that come from SSR. Hence force a re-render after mounting to apply the
  // current relevant styles. There will be a flash seen of the original
  // styles seen using this current approach but that's probably ok. Fixing
  // the flash will require changing the theming approach and is not worth it
  // at this point.
  useEffect(() => {
    setMounted(true);
  }, []);

  const button = useRef(null);

  const prismTheme = usePrismTheme();

  // In case interleaved Markdown (e.g. when using CodeBlock as standalone component).
  const content = Array.isArray(children) ? children.join('') : children;

  let language = languageClassName && (languageClassName.replace(/language-/, ''));
  if (!language && prism.defaultLanguage) {
    language = prism.defaultLanguage;
  }

  // only declaration OR directive highlight can be used for a block
  let code = content.replace(/\n$/, '');

  const handleCopyCode = () => {
    copy(code);
    setShowCopied(true);

    setTimeout(() => setShowCopied(false), 2000);
  };

  return (
    <Highlight {...defaultProps} key={mounted} theme={prismTheme} code={code} language={language}>
      {({ className, style, tokens, getLineProps, getTokenProps }) => (
        <div className={styles.codeBlockContainer}>
          <div className={clsx(styles.codeBlockContent, language)}>
            <pre className={`${className}`} style={style}>
              {tokens.map((line, i) => (
                <div {...getLineProps({ line, key: i })}>
                  {line.map((token, key) => (
                    <span {...getTokenProps({ token, key })} />
                  ))}
                </div>
              ))}
            </pre>

            <button
              ref={button}
              type="button"
              aria-label={translate({
                id: 'theme.CodeBlock.copyButtonAriaLabel',
                message: 'Copy code to clipboard',
                description: 'The ARIA label for copy code blocks button',
              })}
              className={clsx(styles.copyButton, 'clean-btn')}
              onClick={handleCopyCode}>
              {showCopied ? (
                <Translate
                  id="theme.CodeBlock.copied"
                  description="The copied button label on code blocks">
                  Copied
                </Translate>
              ) : (
                <Translate
                  id="theme.CodeBlock.copy"
                  description="The copy button label on code blocks">
                  Copy
                </Translate>
              )}
            </button>
          </div>
        </div>
      )}
    </Highlight>
  );
}
